use std::{collections::BTreeMap, fmt::Write, path::PathBuf, str::FromStr, time::Duration};

use chrono::{DateTime, Utc};
use console::style;
use indicatif::{MultiProgress, ProgressBar, ProgressState, ProgressStyle};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncWriteExt, BufWriter};

/// Executes the download command.
pub async fn execute() -> anyhow::Result<()> {
    let spinner = ProgressBar::new_spinner()
        .with_style(ProgressStyle::with_template("{spinner:.green} {wide_msg}").unwrap());
    spinner.enable_steady_tick(Duration::from_millis(100));

    spinner.set_message("Downloading dumps list");

    let client = Client::builder()
        .user_agent("wiki-scraper-zeramorphic")
        .build()?;

    // Obtain a list of the most recent available file dumps, e.g.
    // ["20240301/", "20240320/", "20240401/", "20240420/", "20240501/", "20240601/", "20240620/", "latest/"]
    let response = client
        .get("https://dumps.wikimedia.org/enwiki/")
        .send()
        .await?
        .error_for_status()?;
    let file_names = crate::parse::parse_html_index::file_names(&response.text().await?)?;

    // Iterate through the dumps in reverse order until we find a dump that's already finished.
    // This way we're always looking at the most recent completed dump.
    for dir in file_names.into_iter().rev() {
        let dir = dir.trim_end_matches('/');
        if dir.contains("latest") {
            continue;
        }
        spinner.set_message(format!(
            "Downloading dump information for version {}",
            style(dir).bright().bold()
        ));
        let response = client
            .get(format!(
                "https://dumps.wikimedia.org/enwiki/{dir}/dumpstatus.json"
            ))
            .send()
            .await?
            .error_for_status()?;
        let text = response.text().await?;
        let dump_status = serde_json::from_str::<DumpStatus>(&text)?;

        if dump_status.jobs.done() {
            spinner.finish_with_message(format!("Using version {}", style(dir).bright().bold()));
            return execute_dump(&client, dump_status).await;
        }
    }

    Err(anyhow::Error::msg("no dump found"))
}

/// Download this completed dump.
async fn execute_dump(client: &Client, dump_status: DumpStatus) -> anyhow::Result<()> {
    tokio::fs::create_dir_all("data").await?;
    tokio::fs::write(
        "data/current_dump.json",
        serde_json::to_string_pretty(&dump_status)?,
    )
    .await?;

    let multi_progress = MultiProgress::new();

    let all_files = dump_status.jobs.all_files();

    let main_progress = ProgressBar::new(all_files.len() as u64)
        .with_style(ProgressStyle::with_template("[{pos}/{len}] {wide_msg}").unwrap());
    multi_progress.add(main_progress.clone());

    for (file, status) in all_files {
        main_progress.set_message(format!("Downloading {file}"));
        let file_progress = ProgressBar::new(status.size);
        file_progress.set_style(ProgressStyle::with_template("{spinner:.green} {msg} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta_precise})")
            .unwrap()
            .with_key("eta", |state: &ProgressState, w: &mut dyn Write| write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap())
            .progress_chars("#>-"));
        file_progress.enable_steady_tick(Duration::from_millis(100));
        download_file(client, &file, &status, &file_progress).await?;
        main_progress.inc(1);
        multi_progress.remove(&file_progress);
    }

    main_progress.finish();

    Ok(())
}

async fn download_file(
    client: &Client,
    file: &str,
    status: &FileStatus,
    progress: &ProgressBar,
) -> anyhow::Result<()> {
    let local_path = PathBuf::from_str("data")
        .unwrap()
        .join(&status.url.trim_start_matches('/'));
    if tokio::fs::metadata(&local_path)
        .await
        .is_ok_and(|metadata| metadata.is_file())
    {
        // We already downloaded the file; exit early.
        return Ok(());
    }

    let url = format!("https://dumps.wikimedia.org/{}", status.url);
    let mut response = client.get(url).send().await?.error_for_status()?;

    // The response succeeded, so let's create the local file.
    tokio::fs::create_dir_all(local_path.parent().unwrap()).await?;
    let output = tokio::fs::File::create(local_path).await?;
    let mut writer = BufWriter::new(output);

    let mut md5_context = md5::Context::new();

    while let Some(chunk) = response.chunk().await? {
        progress.inc(chunk.len() as u64);
        writer.write_all(&chunk).await?;
        md5_context.consume(chunk);
    }

    let digest = format!("{:x}", md5_context.compute());
    assert_eq!(status.md5, digest);

    writer.flush().await?;
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct DumpStatus {
    jobs: JobsStatus,
    version: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct JobsStatus {
    #[serde(rename = "sitestatstable")]
    site_stats: JobStatus,
    #[serde(rename = "articlesmultistreamdump")]
    articles_multistream_dump: JobStatus,
}

impl JobsStatus {
    pub fn done(&self) -> bool {
        self.articles_multistream_dump.done() && self.site_stats.done()
    }

    pub fn all_files(&self) -> Vec<(String, FileStatus)> {
        vec![
            self.site_stats.files(),
            self.articles_multistream_dump.files(),
        ]
        .into_iter()
        .flatten()
        .collect()
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "lowercase")]
enum JobStatus {
    Done {
        updated: CustomDateTime,
        files: BTreeMap<String, FileStatus>,
    },
    Waiting {},
}

impl JobStatus {
    pub fn done(&self) -> bool {
        matches!(self, JobStatus::Done { .. })
    }

    pub fn files(&self) -> Vec<(String, FileStatus)> {
        match self {
            JobStatus::Done { files, .. } => files
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            JobStatus::Waiting {} => Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FileStatus {
    size: u64,
    url: String,
    md5: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct CustomDateTime(#[serde(with = "custom_date_format")] DateTime<Utc>);

#[allow(dead_code)]
mod custom_date_format {
    use chrono::{DateTime, NaiveDateTime, Utc};
    use serde::{self, Deserialize, Deserializer, Serializer};

    const FORMAT: &'static str = "%Y-%m-%d %H:%M:%S";

    pub fn serialize<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}", date.format(FORMAT));
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let dt = NaiveDateTime::parse_from_str(&s, FORMAT).map_err(serde::de::Error::custom)?;
        Ok(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))
    }
}