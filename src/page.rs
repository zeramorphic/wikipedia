use std::{collections::HashMap, fmt::Debug, path::PathBuf, str::FromStr, sync::Arc};

use async_compression::tokio::bufread::BzDecoder;
use bimap::BiMap;
use chrono::{DateTime, FixedOffset};
use console::style;
use serde::{Deserialize, Serialize};
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, BufReader},
    sync::RwLock,
};

use crate::{
    commands::download::DumpStatus,
    memoise::memoise,
    parse::xml::{make_errors_static, parse_element, parse_whitespace, shorten, Element},
    progress_bar::normal_progress_bar,
};

/// Yields some `'static` information about every page.
/// The `capacity` is the capacity of the internal buffer.
pub async fn page_stream<T: Send + Sync + 'static>(
    cutoff: u64,
    capacity: usize,
    message: String,
    information: impl for<'a> Fn(ParsedPage<'a>) -> T + Clone + Send + 'static,
) -> anyhow::Result<async_channel::Receiver<T>> {
    let dump_status = get_dump_status().await?;

    let num_articles = count_articles(&dump_status).await?;
    num_articles.summarise();

    let max = if cutoff < num_articles.total() {
        println!(
            "Processing the first {} articles",
            style(cutoff).bold().bright()
        );
        cutoff
    } else {
        num_articles.total()
    };

    let progress_bar = normal_progress_bar(max).with_message(message);

    let (tx, rx) = async_channel::bounded(capacity);

    let files = dump_status.jobs.articles_multistream_dump.files();
    for (_, articles) in files.iter().filter(|(file, _)| !file.contains("index")) {
        let progress_bar = progress_bar.clone();
        let articles = articles.clone();
        let tx = tx.clone();
        let information = information.clone();
        tokio::task::spawn(async move {
            let mut articles_file =
                tokio::fs::File::open(PathBuf::from_str("data")?.join(&articles.url)).await?;
            let articles_index_file = tokio::fs::File::open(
                PathBuf::from_str("data")?.join(
                    &articles
                        .url
                        .replace("multistream", "multistream-index")
                        .replace(".xml", ".txt"),
                ),
            )
            .await?;

            let mut lines =
                BufReader::new(BzDecoder::new(BufReader::new(articles_index_file))).lines();
            let mut latest_offset = 0;

            while let Some(line) = lines.next_line().await? {
                if line.is_empty() {
                    continue;
                }

                let (byte_offset, line) = line.split_once(':').unwrap();
                let (_article_id, _article_title) = line.split_once(':').unwrap();
                let byte_offset = byte_offset.parse::<u64>()?;

                if byte_offset > latest_offset {
                    latest_offset = byte_offset;
                    let pages = read_pages(&mut articles_file, byte_offset).await?;
                    let mut input = pages.as_str();
                    while !input.is_empty() {
                        let (new_input, _) = make_errors_static(parse_whitespace(input))?;
                        let (new_input, page) = make_errors_static(parse_element(new_input))?;
                        let (new_input, _) = make_errors_static(parse_whitespace(new_input))?;
                        input = new_input;
                        tx.send(information(ParsedPage::from(page))).await?;
                        progress_bar.inc(1);
                        if progress_bar.position() >= max {
                            return Ok(());
                        }
                    }
                }
            }

            Ok::<(), anyhow::Error>(())
        });
    }

    Ok(rx)
}

pub async fn get_dump_status() -> anyhow::Result<DumpStatus> {
    Ok(serde_json::from_str::<DumpStatus>(
        &tokio::fs::read_to_string("data/current_dump.json").await?,
    )?)
}

pub async fn count_articles(dump_status: &DumpStatus) -> anyhow::Result<ArticleCount> {
    memoise("article_count", "Counting articles", false, || async {
        let mut output = ArticleCount::default();
        let files: Vec<(String, crate::commands::download::FileStatus)> =
            dump_status.jobs.articles_multistream_dump.files();
        let progress_bar = normal_progress_bar(
            files
                .iter()
                .filter(|(file, _)| file.contains("index"))
                .count() as u64,
        )
        .with_message("Counting articles");
        for (file, articles) in files.iter().filter(|(file, _)| file.contains("index")) {
            let articles_index_file =
                tokio::fs::File::open(PathBuf::from_str("data")?.join(&articles.url)).await?;
            let mut lines =
                BufReader::new(BzDecoder::new(BufReader::new(articles_index_file))).lines();
            let mut num_articles = 0u64;
            while let Some(line) = lines.next_line().await? {
                if line.is_empty() {
                    continue;
                }
                num_articles += 1;
            }
            output
                .articles_per_stream
                .insert(file.to_owned(), num_articles);
            progress_bar.inc(1);
        }
        progress_bar.finish();
        Ok(output)
    })
    .await
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ArticleCount {
    pub articles_per_stream: HashMap<String, u64>,
}

impl ArticleCount {
    pub fn summarise(&self) {
        println!(
            "Found a total of {} articles over {} streams",
            style(self.total()).bold().bright(),
            style(self.articles_per_stream.len()).bold().bright()
        );
    }

    pub fn total(&self) -> u64 {
        self.articles_per_stream.values().sum()
    }
}

pub async fn id_to_title() -> anyhow::Result<BiMap<u32, String>> {
    memoise("id_to_title", "Precomputing page IDs", true, || async {
        let id_to_title = Arc::new(RwLock::new(BiMap::<u32, String>::new()));

        let rx =
            crate::page::page_stream(u64::MAX, 1, "Precomputing page IDs".to_owned(), |page| {
                (page.id, page.title.to_owned())
            })
            .await?;

        while let Ok((id, title)) = rx.recv().await {
            id_to_title.write().await.insert(id, title);
        }

        let mut result = id_to_title.write().await;
        let result: &mut BiMap<u32, String> = &mut result;
        Ok(std::mem::take(result))
    })
    .await
}

/// Reads the pages at the given byte offset in the supplied articles file.
/// There are normally 100 pages in each substream.
async fn read_pages(articles_file: &mut File, byte_offset: u64) -> anyhow::Result<String> {
    articles_file
        .seek(std::io::SeekFrom::Start(byte_offset))
        .await?;
    let mut decoder = BzDecoder::new(BufReader::new(articles_file));
    let mut output = String::new();
    decoder.read_to_string(&mut output).await?;
    Ok(output)
}

/// We use custom XML deserialisation for pages because of how important efficiency is for our use-case.
#[derive(Default, Debug)]
pub struct ParsedPage<'a> {
    pub title: &'a str,
    pub namespace: u32,
    pub id: u32,
    pub redirect: Option<&'a str>,
    pub revision: ParsedRevision<'a>,
}

#[derive(Default)]
pub struct ParsedRevision<'a> {
    pub id: u32,
    pub timestamp: DateTime<FixedOffset>,
    pub model: &'a str,
    pub format: &'a str,
    pub text: &'a str,
}

impl<'a> From<Element<'a>> for ParsedPage<'a> {
    fn from(value: Element<'a>) -> Self {
        let mut result = Self::default();
        for child in value.children {
            match child.name {
                "title" => result.title = child.text,
                "ns" => result.namespace = child.text.parse().unwrap(),
                "id" => result.id = child.text.parse().unwrap(),
                "redirect" => result.redirect = Some(child.get_attribute("title").unwrap()),
                "revision" => result.revision = ParsedRevision::from(child),
                _ => todo!("unrecognised page child {}", child.summarise()),
            }
        }
        result
    }
}

impl<'a> Debug for ParsedRevision<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParsedRevision")
            .field("id", &self.id)
            .field("timestamp", &self.timestamp)
            .field("model", &self.model)
            .field("format", &self.format)
            .field("text", &shorten(self.text.to_owned()))
            .finish()
    }
}

impl<'a> From<Element<'a>> for ParsedRevision<'a> {
    fn from(value: Element<'a>) -> Self {
        let mut result = Self::default();
        for child in value.children {
            match child.name {
                "id" => result.id = child.text.parse().unwrap(),
                "timestamp" => {
                    result.timestamp = DateTime::parse_from_rfc3339(&child.text).unwrap()
                }
                "model" => result.model = child.text,
                "format" => result.format = child.text,
                "text" => result.text = child.text,
                "parentid" | "contributor" | "comment" | "origin" | "sha1" | "minor" => {}
                _ => todo!("unrecognised revision child {}", child.summarise()),
            }
        }
        result
    }
}
