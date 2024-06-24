use std::{
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use async_compression::tokio::{bufread::GzipDecoder, write::GzipEncoder};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, BufReader, BufWriter};
use tokio_util::io::SyncIoBridge;

use crate::progress_bar::file_progress_bar;

/// Stores the result of this function on disk and retrieves it when needed.
pub async fn memoise<T, F>(
    key: &str,
    name: &str,
    gz: bool,
    f: impl FnOnce() -> F,
) -> anyhow::Result<T>
where
    T: Serialize + Send + for<'a> Deserialize<'a> + 'static,
    F: Future<Output = anyhow::Result<T>>,
{
    if let Ok(file) =
        tokio::fs::File::open(format!("data/{key}.json{}", if gz { ".gz" } else { "" })).await
    {
        let len = file.metadata().await?.len();
        let progress = Arc::new(AtomicUsize::new(0));
        let progress2 = Arc::clone(&progress);
        let mut task = tokio::task::spawn_blocking(move || {
            if gz {
                let reader = SyncIoBridge::new(GzipDecoder::new(BufReader::new(
                    ReadProgressHook::new(file, progress2),
                )));
                Ok(serde_json::from_reader(reader)?)
            } else {
                let reader =
                    SyncIoBridge::new(BufReader::new(ReadProgressHook::new(file, progress2)));
                Ok(serde_json::from_reader(reader)?)
            }
        });
        let progress_bar = file_progress_bar(len).with_message(format!("{name} (cached)"));
        loop {
            let result = tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(100)) => {
                    progress_bar.set_position(progress.load(Ordering::SeqCst) as u64);
                    None
                },
                result = &mut task => {
                    Some(result)
                },
            };
            if let Some(result) = result {
                progress_bar.finish();
                return result?;
            }
        }
    } else {
        let result = f().await?;
        let file =
            tokio::fs::File::create(format!("data/{key}.json{}", if gz { ".gz" } else { "" }))
                .await?;
        tokio::task::spawn_blocking(move || {
            if gz {
                let mut encoder = SyncIoBridge::new(GzipEncoder::new(BufWriter::new(file)));
                serde_json::to_writer(&mut encoder, &result)?;
                encoder.shutdown()?;
                Ok(result)
            } else {
                let mut encoder = SyncIoBridge::new(BufWriter::new(file));
                serde_json::to_writer(&mut encoder, &result)?;
                encoder.shutdown()?;
                Ok(result)
            }
        })
        .await?
    }
}

#[pin_project::pin_project]
struct ReadProgressHook<R> {
    #[pin]
    inner: R,
    progress: Arc<AtomicUsize>,
}

impl<R> ReadProgressHook<R> {
    pub fn new(inner: R, progress: Arc<AtomicUsize>) -> Self {
        Self { inner, progress }
    }
}

impl<R> AsyncRead for ReadProgressHook<R>
where
    R: AsyncRead,
{
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.project();
        let unfilled = buf.filled().len();
        let result = this.inner.poll_read(cx, buf);
        let difference = buf.filled().len() - unfilled;
        this.progress
            .fetch_add(difference, std::sync::atomic::Ordering::SeqCst);
        result
    }
}
