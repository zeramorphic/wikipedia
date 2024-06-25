use std::{
    collections::BTreeMap,
    io::{BufReader, BufWriter, Read, Write},
    path::PathBuf,
    str::FromStr,
    time::Duration,
};

use bzip2::bufread::BzDecoder;
use chrono::{DateTime, Utc};
use console::style;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use ureq::{Agent, AgentBuilder};

use crate::progress_bar::file_progress_bar;

/// Executes the download command.
pub fn execute(date: Option<String>) -> anyhow::Result<()> {
    let spinner = ProgressBar::new_spinner()
        .with_style(ProgressStyle::with_template("{spinner:.green} {wide_msg}").unwrap());
    spinner.enable_steady_tick(Duration::from_millis(100));

    spinner.set_message("Downloading dumps list");

    let agent = AgentBuilder::new()
        .user_agent("wiki-scraper-zeramorphic")
        .build();

    match date {
        Some(date) => {
            spinner.set_message(format!(
                "Downloading dump information for version {}",
                style(&date).bright().bold()
            ));
            let response = agent
                .get(&format!(
                    "https://dumps.wikimedia.org/enwiki/{date}/dumpstatus.json"
                ))
                .call()?;
            let text = response.into_string()?;
            let mut dump_status = serde_json::from_str::<DumpStatus>(&text)?;
            dump_status.fix_paths();
            dump_status.date = Some(date.clone());

            assert!(dump_status.jobs.done());
            spinner.finish_with_message(format!("Using version {}", style(date).bright().bold()));
            execute_dump(&agent, dump_status)
        }
        None => {
            // Obtain a list of the most recent available file dumps, e.g.
            // ["20240301/", "20240320/", "20240401/", "20240420/", "20240501/", "20240601/", "20240620/", "latest/"]
            let response = agent.get("https://dumps.wikimedia.org/enwiki/").call()?;
            let file_names = crate::parse::parse_html_index::file_names(&response.into_string()?)?;

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
                let response = agent
                    .get(&format!(
                        "https://dumps.wikimedia.org/enwiki/{dir}/dumpstatus.json"
                    ))
                    .call()?;
                let text = response.into_string()?;
                let mut dump_status = serde_json::from_str::<DumpStatus>(&text)?;
                dump_status.fix_paths();
                dump_status.date = Some(dir.to_owned());

                if dump_status.jobs.done() {
                    spinner.finish_with_message(format!(
                        "Using version {}",
                        style(dir).bright().bold()
                    ));
                    return execute_dump(&agent, dump_status);
                }
            }

            Err(anyhow::Error::msg("no dump found"))
        }
    }
}

/// Download this completed dump.
fn execute_dump(agent: &Agent, dump_status: DumpStatus) -> anyhow::Result<()> {
    std::fs::create_dir_all("data")?;
    std::fs::write(
        "data/current_dump.json",
        serde_json::to_string_pretty(&dump_status)?,
    )?;

    let multi_progress = MultiProgress::new();

    let all_files = dump_status.jobs.all_files();

    let main_progress = ProgressBar::new(all_files.len() as u64)
        .with_style(ProgressStyle::with_template("[{pos}/{len}] {wide_msg}").unwrap());
    multi_progress.add(main_progress.clone());

    for (file, status) in all_files {
        main_progress.set_message(format!("Downloading {file}"));
        let file_progress = file_progress_bar(status.size);
        download_file(agent, &status, &file_progress)?;
        main_progress.inc(1);
        multi_progress.remove(&file_progress);
    }

    main_progress.finish();

    Ok(())
}

fn download_file(agent: &Agent, status: &FileStatus, progress: &ProgressBar) -> anyhow::Result<()> {
    // Special case: BZ2-decompress index files.
    let is_index = status.url.contains("index");

    let mut local_path = PathBuf::from_str("data").unwrap().join(&status.url);
    if is_index {
        local_path.set_extension("txt");
    };
    if std::fs::metadata(&local_path).is_ok_and(|metadata| metadata.is_file()) {
        // We already downloaded the file; exit early.
        return Ok(());
    }

    let url = format!("https://dumps.wikimedia.org/{}", status.url);
    let response = agent.get(&url).call()?;

    // The response succeeded, so let's create the local file.
    std::fs::create_dir_all(local_path.parent().unwrap())?;
    let output = std::fs::File::create(local_path)?;
    let mut writer = BufWriter::new(output);

    let mut md5_context = md5::Context::new();
    let mut reader: Box<dyn Read> = if is_index {
        Box::new(BufReader::new(BzDecoder::new(BufReader::new(
            response.into_reader(),
        ))))
    } else {
        Box::new(BufReader::new(response.into_reader()))
    };
    let mut buf = vec![0u8; 0x10000];
    loop {
        let bytes_read = reader.read(&mut buf)?;
        if bytes_read == 0 {
            break;
        }
        progress.inc(bytes_read as u64);
        writer.write_all(&buf[0..bytes_read])?;
        md5_context.consume(&buf[0..bytes_read]);
    }

    let digest = format!("{:x}", md5_context.compute());
    if !is_index {
        // For now we just ignore the MD5 hash of index files, because
        // we're actually calculating the decompressed digest.
        assert_eq!(status.md5, digest);
    }

    writer.flush()?;
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DumpStatus {
    pub date: Option<String>,
    pub jobs: JobsStatus,
    pub version: String,
}

impl DumpStatus {
    pub fn fix_paths(&mut self) {
        self.jobs.fix_paths();
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JobsStatus {
    #[serde(rename = "sitestatstable")]
    pub site_stats: JobStatus,
    #[serde(rename = "allpagetitlesdump")]
    pub all_page_titles_dump: JobStatus,
    #[serde(rename = "articlesmultistreamdump")]
    pub articles_multistream_dump: JobStatus,
}

impl JobsStatus {
    pub fn done(&self) -> bool {
        self.site_stats.done()
            && self.all_page_titles_dump.done()
            && self.articles_multistream_dump.done()
    }

    pub fn fix_paths(&mut self) {
        self.site_stats.fix_paths();
        self.all_page_titles_dump.fix_paths();
        self.articles_multistream_dump.fix_paths();
    }

    pub fn all_files(&self) -> Vec<(String, FileStatus)> {
        vec![
            self.site_stats.files(),
            self.all_page_titles_dump.files(),
            self.articles_multistream_dump.files(),
        ]
        .into_iter()
        .flatten()
        .collect()
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "lowercase")]
pub enum JobStatus {
    Done {
        updated: WikiDateTime,
        files: BTreeMap<String, FileStatus>,
    },
    Waiting {},
}

impl JobStatus {
    pub fn done(&self) -> bool {
        matches!(self, JobStatus::Done { .. })
    }

    pub fn fix_paths(&mut self) {
        match self {
            JobStatus::Done { files, .. } => {
                for file in files.values_mut() {
                    file.url = file.url.trim_start_matches('/').to_owned();
                }
            }
            JobStatus::Waiting {} => {}
        }
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
pub struct FileStatus {
    pub size: u64,
    pub url: String,
    pub md5: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct WikiDateTime(#[serde(with = "custom_date_format")] pub DateTime<Utc>);

#[allow(dead_code)]
mod custom_date_format {
    use chrono::{DateTime, NaiveDateTime, Utc};
    use serde::{self, Deserialize, Deserializer, Serializer};

    const FORMAT: &str = "%Y-%m-%d %H:%M:%S";

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
