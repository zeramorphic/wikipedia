use std::{
    io::{BufReader, BufWriter, Read, Write},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use flate2::{
    bufread::{GzDecoder, GzEncoder},
    Compression,
};
use serde::{Deserialize, Serialize};

use crate::progress_bar::file_progress_bar;

/// Stores the result of this function on disk and retrieves it when needed.
pub fn memoise<T>(
    key: &str,
    name: &str,
    gz: bool,
    f: impl FnOnce() -> anyhow::Result<T>,
) -> anyhow::Result<T>
where
    T: Serialize + for<'a> Deserialize<'a> + Send + 'static,
{
    if let Ok(file) = std::fs::File::open(format!("data/{key}.json{}", if gz { ".gz" } else { "" }))
    {
        let len = file.metadata()?.len();
        let progress = Arc::new(AtomicUsize::new(0));
        let progress2 = Arc::clone(&progress);
        let task = std::thread::spawn(move || {
            if gz {
                let reader = GzDecoder::new(BufReader::new(ReadProgressHook::new(file, progress2)));
                Ok(serde_json::from_reader(reader)?)
            } else {
                let reader = BufReader::new(ReadProgressHook::new(file, progress2));
                Ok(serde_json::from_reader(reader)?)
            }
        });
        let progress_bar = file_progress_bar(len).with_message(format!("{name} (cached)"));
        while !task.is_finished() {
            std::thread::sleep(Duration::from_millis(100));
            progress_bar.set_position(progress.load(Ordering::SeqCst) as u64);
        }
        progress_bar.finish();
        task.join().map_err(|_| anyhow::Error::msg("panic"))?
    } else {
        let result = f()?;
        let file =
            std::fs::File::create(format!("data/{key}.json{}", if gz { ".gz" } else { "" }))?;

        if gz {
            let (reader, mut writer) = pipe::pipe();
            let task = std::thread::spawn::<_, anyhow::Result<()>>(move || {
                let mut encoder = GzEncoder::new(reader, Compression::best());
                let mut writer = BufWriter::new(file);
                std::io::copy(&mut encoder, &mut writer)?;
                writer.flush()?;
                Ok(())
            });
            serde_json::to_writer(&mut writer, &result)?;
            task.join().map_err(|_| anyhow::Error::msg("panic"))??;
            Ok(result)
        } else {
            let (mut reader, mut writer) = pipe::pipe();
            let task = std::thread::spawn::<_, anyhow::Result<()>>(move || {
                let mut writer = BufWriter::new(file);
                std::io::copy(&mut reader, &mut writer)?;
                writer.flush()?;
                Ok(())
            });
            serde_json::to_writer(&mut writer, &result)?;
            task.join().map_err(|_| anyhow::Error::msg("panic"))??;
            Ok(result)
        }
    }
}

/// Stores the result of this function on disk and retrieves it when needed.
pub fn memoise_bytes<T>(
    key: &str,
    name: &str,
    gz: bool,
    f: impl FnOnce() -> anyhow::Result<T>,
) -> anyhow::Result<T>
where
    T: BytesSerde + Send + 'static,
{
    if let Ok(file) = std::fs::File::open(format!("data/{key}.bin{}", if gz { ".gz" } else { "" }))
    {
        let len = file.metadata()?.len();
        let progress = Arc::new(AtomicUsize::new(0));
        let progress2 = Arc::clone(&progress);
        let task = std::thread::spawn(move || {
            if gz {
                let mut reader =
                    GzDecoder::new(BufReader::new(ReadProgressHook::new(file, progress2)));
                Ok(<T as BytesSerde>::deserialize(&mut reader)?)
            } else {
                let mut reader = BufReader::new(ReadProgressHook::new(file, progress2));
                Ok(<T as BytesSerde>::deserialize(&mut reader)?)
            }
        });
        let progress_bar = file_progress_bar(len).with_message(format!("{name} (cached)"));
        while !task.is_finished() {
            std::thread::sleep(Duration::from_millis(100));
            progress_bar.set_position(progress.load(Ordering::SeqCst) as u64);
        }
        progress_bar.finish();
        task.join().map_err(|_| anyhow::Error::msg("panic"))?
    } else {
        let result = f()?;
        let file = std::fs::File::create(format!("data/{key}.bin{}", if gz { ".gz" } else { "" }))?;

        if gz {
            let (reader, mut writer) = pipe::pipe();
            let task = std::thread::spawn::<_, anyhow::Result<()>>(move || {
                let mut encoder = GzEncoder::new(reader, Compression::best());
                let mut writer = BufWriter::new(file);
                std::io::copy(&mut encoder, &mut writer)?;
                writer.flush()?;
                Ok(())
            });
            result.serialize(&mut writer)?;
            task.join().map_err(|_| anyhow::Error::msg("panic"))??;
            Ok(result)
        } else {
            let (mut reader, mut writer) = pipe::pipe();
            let task = std::thread::spawn::<_, anyhow::Result<()>>(move || {
                let mut writer = BufWriter::new(file);
                std::io::copy(&mut reader, &mut writer)?;
                writer.flush()?;
                Ok(())
            });
            result.serialize(&mut writer)?;
            task.join().map_err(|_| anyhow::Error::msg("panic"))??;
            Ok(result)
        }
    }
}

/// A trait for more efficient serialisation and deserialisation mechanisms.
pub trait BytesSerde: Sized {
    fn serialize(&self, writer: &mut impl std::io::Write) -> anyhow::Result<()>;
    fn deserialize(reader: &mut impl std::io::Read) -> anyhow::Result<Self>;
}

struct ReadProgressHook<R> {
    inner: R,
    progress: Arc<AtomicUsize>,
}

impl<R> ReadProgressHook<R> {
    pub fn new(inner: R, progress: Arc<AtomicUsize>) -> Self {
        Self { inner, progress }
    }
}

impl<R> Read for ReadProgressHook<R>
where
    R: Read,
{
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let result = self.inner.read(buf)?;
        self.progress
            .fetch_add(result, std::sync::atomic::Ordering::SeqCst);
        Ok(result)
    }
}
