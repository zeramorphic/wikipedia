use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
    fs::File,
    io::{BufRead, BufReader, BufWriter, Write},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
};

use crossbeam::channel::Receiver;
use serde::{Deserialize, Serialize};

use crate::{binary_search_line::binary_search_line_in_file, progress_bar};

type LockedBTreeMap<K, V> = Arc<RwLock<BTreeMap<K, V>>>;

/// A nested map type, associating values of type `V` to keys of type `L`.
/// A "short key" of type `K` is derived from each key of type `L`,
/// and this "short key" is used to partition the main map into many smaller maps,
/// which can be locked and (de)serialised separately.
///
/// We use [`BTreeMap`] internally so that the serialised output is consistent between runs,
/// and is easy to inspect if something goes wrong.
pub struct HierarchicalMap<K, L, V> {
    /// The prefix we use for (de)serializing this map.
    prefix: PathBuf,

    /// Whether this hierarchical map has been fully loaded from disk.
    fully_loaded: Arc<AtomicBool>,

    #[allow(clippy::type_complexity)]
    shorten: Arc<Box<dyn Fn(&L) -> K + Send + Sync + 'static>>,
    map: LockedBTreeMap<K, LockedBTreeMap<L, V>>,
}

impl<K, L, V> Clone for HierarchicalMap<K, L, V> {
    fn clone(&self) -> Self {
        Self {
            prefix: self.prefix.clone(),
            fully_loaded: self.fully_loaded.clone(),
            shorten: self.shorten.clone(),
            map: self.map.clone(),
        }
    }
}

impl<K, L, V> Debug for HierarchicalMap<K, L, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "<hierarchical map using {} total keys and {} short keys>",
            self.total_keys(),
            self.total_short_keys()
        )
    }
}

impl<K, L, V> Display for HierarchicalMap<K, L, V>
where
    K: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Hierarchical map using {} total keys and {} short keys:",
            self.total_keys(),
            self.total_short_keys()
        )?;

        let mut keys_sorted = self
            .map
            .read()
            .unwrap()
            .iter()
            .map(|(key, val)| (key.to_string(), val.read().unwrap().len()))
            .collect::<Vec<_>>();
        keys_sorted.sort_by_key(|(_, n)| *n);

        writeln!(f, "* Overutilised short keys:")?;
        for (short_key, n) in keys_sorted.iter().rev().take(5) {
            writeln!(f, "  - {n} entries: {short_key}")?;
        }

        writeln!(f, "* Underutilised short keys:")?;
        for (short_key, n) in keys_sorted.iter().take(5) {
            writeln!(f, "  - {n} entries: {short_key}")?;
        }

        Ok(())
    }
}

impl<K, L, V> HierarchicalMap<K, L, V> {
    pub fn new(prefix: PathBuf, shorten: impl Fn(&L) -> K + Send + Sync + 'static) -> Self {
        Self {
            prefix,
            fully_loaded: Arc::new(AtomicBool::new(false)),
            shorten: Arc::new(Box::new(shorten)),
            map: LockedBTreeMap::default(),
        }
    }

    pub fn is_fully_loaded(&self) -> bool {
        self.fully_loaded.load(Ordering::SeqCst)
    }

    pub fn mark_loaded(&self) {
        self.fully_loaded.store(true, Ordering::SeqCst);
    }

    pub fn total_short_keys(&self) -> usize {
        self.map.read().unwrap().len()
    }

    pub fn total_keys(&self) -> usize {
        self.map
            .read()
            .unwrap()
            .values()
            .map(|inner_map| inner_map.read().unwrap().len())
            .sum()
    }

    /// Inserts the given key-value pair into this hierarchical map.
    pub fn insert(&self, key: L, value: V) -> Option<V>
    where
        K: Ord,
        L: Ord,
    {
        let short_key = (self.shorten)(&key);
        let guard = self.map.read().unwrap();
        match guard.get(&short_key) {
            Some(inner_map) => inner_map.write().unwrap().insert(key, value),
            None => {
                // This is the expensive path.
                // We need to reacquire the lock on the outer map in write mode.
                // It's possible that some other thread added this entry in the meantime,
                // so we need to use `or_default` here.
                drop(guard);
                self.map
                    .write()
                    .unwrap()
                    .entry(short_key)
                    .or_default()
                    .write()
                    .unwrap()
                    .insert(key, value)
            }
        }
    }

    /// Mutates the given key-value pair,
    /// starting with a default value if the key is not already in the map.
    pub fn mutate_with_default(&self, key: L, mutate: impl FnOnce(&mut V))
    where
        K: Ord,
        L: Ord,
        V: Default,
    {
        let short_key = (self.shorten)(&key);
        let guard = self.map.read().unwrap();
        match guard.get(&short_key) {
            Some(inner_map) => mutate(inner_map.write().unwrap().entry(key).or_default()),
            None => {
                drop(guard);
                mutate(
                    self.map
                        .write()
                        .unwrap()
                        .entry(short_key)
                        .or_default()
                        .write()
                        .unwrap()
                        .entry(key)
                        .or_default(),
                )
            }
        }
    }

    /// Obtains the value associated to the given key, applies `f` to it, and returns the result.
    /// If the key was not found, we check the cache on disk, and add the key-value pair to `self`.
    /// If the key was not found, and it cannot be found on disk, this returns [`None`].
    pub fn with<T>(&self, key: &L, f: impl FnOnce(&V) -> T) -> Option<T>
    where
        K: Ord + Display,
        L: Ord + Clone + for<'a> Deserialize<'a>,
        V: for<'a> Deserialize<'a>,
    {
        let short_key = (self.shorten)(key);
        let outer_guard = self.map.read().unwrap();
        let inner_map = outer_guard.get(&short_key);
        if let Some(inner_map) = inner_map {
            if let Some(value) = inner_map.read().unwrap().get(key) {
                return Some(f(value));
            }
        }
        drop(outer_guard);
        if self.is_fully_loaded() {
            return None;
        }

        // Try to load this key-value pair from disk.
        let prefix = PathBuf::from("data").join(&self.prefix);
        let mut file =
            match std::fs::File::open(prefix.join(short_key.to_string()).with_extension("jsonl")) {
                Ok(file) => file,
                Err(_) => return None,
            };

        // Now perform a binary search in the file to try to find the right key.
        match find_entry_in_file(&mut file, key) {
            Ok(Some(value)) => {
                let result = f(&value);
                self.insert(key.clone(), value);
                Some(result)
            }
            Ok(None) => None,
            Err(err) => panic!("{}\n{}", err, err.backtrace()),
        }
    }

    pub fn with_all<T>(
        &self,
        message: String,
        f: impl Fn(&L, &V) -> T + Send + 'static,
    ) -> Receiver<T>
    where
        K: Send + Sync + 'static,
        L: Send + Sync + 'static,
        V: Send + Sync + 'static,
        T: Send + Sync + 'static,
    {
        assert!(self.is_fully_loaded());
        let (tx, rx) = crossbeam::channel::bounded(1);
        let this = self.clone();
        std::thread::spawn::<_, anyhow::Result<()>>(move || {
            let progress_bar =
                progress_bar::normal_progress_bar(this.total_keys() as u64).with_message(message);
            for inner_map in this.map.read().unwrap().values() {
                for (key, value) in inner_map.read().unwrap().iter() {
                    tx.send(f(key, value))?;
                    progress_bar.inc(1);
                }
            }
            Ok(())
        });
        rx
    }

    /// Returns the underying map.
    pub fn get_map(&self) -> &LockedBTreeMap<K, LockedBTreeMap<L, V>> {
        &self.map
    }

    /// Serialises this hierarchical map using `self.prefix`, which should be something like `folder/information`.
    /// The output will be a file of the form `folder/information.json`, and a folder `folder/information/` which
    /// will contain a `jsonl` file for each short key used.
    pub fn serialize(&self) -> anyhow::Result<()>
    where
        K: Send + Sync + Serialize + Display,
        L: Send + Sync + Serialize + 'static,
        V: Send + Sync + Serialize + 'static,
    {
        if !self.is_fully_loaded() {
            panic!("hierarchical map not fully loaded before serialising");
        }

        let prefix = PathBuf::from("data").join(&self.prefix);
        std::fs::create_dir_all(&prefix)?;
        let map = self.map.read().unwrap();

        // First, serialise the main map data.
        {
            let file = std::fs::File::create(prefix.with_extension("json"))?;
            let mut writer = BufWriter::new(file);
            serde_json::to_writer(&mut writer, &map.keys().collect::<Vec<_>>())?;
            writer.flush()?;
        }

        // Then, serialise all of the inner maps.
        let threads = map
            .iter()
            .map(|(short_key, inner_map)| {
                let prefix = prefix.to_owned();
                let short_key = short_key.to_string();
                let inner_map = Arc::clone(inner_map);
                std::thread::spawn::<_, anyhow::Result<()>>(move || {
                    let file =
                        std::fs::File::create(prefix.join(short_key).with_extension("jsonl"))?;
                    let mut writer = BufWriter::new(file);
                    for (key, value) in inner_map.read().unwrap().iter() {
                        serde_json::to_writer(&mut writer, &(key, value))?;
                        writeln!(writer)?;
                    }
                    writer.flush()?;
                    Ok(())
                })
            })
            .collect::<Vec<_>>();

        for thread in threads {
            thread.join().map_err(|_| anyhow::Error::msg("panic"))??;
        }

        Ok(())
    }

    /// If `full` is false, we'll only deserialise the outermost map, and ignore the inner maps.
    /// If successful, this function returns `Ok(true)`.
    /// If no data has been serialised, this function returns `Ok(false)`.
    pub fn deserialize(&self, full: bool) -> anyhow::Result<bool>
    where
        K: for<'a> Deserialize<'a> + Ord + Display,
        L: Send + Sync + for<'a> Deserialize<'a> + Ord + 'static,
        V: Send + Sync + for<'a> Deserialize<'a> + 'static,
    {
        let prefix = PathBuf::from("data").join(&self.prefix);
        let mut map = self.map.write().unwrap();

        {
            // First, deserialise the main map data.
            let file = match std::fs::File::open(prefix.with_extension("json")) {
                Ok(file) => file,
                Err(_) => return Ok(false),
            };
            let keys: Vec<K> = serde_json::from_reader(BufReader::new(file))?;
            for short_key in keys {
                map.insert(short_key, Default::default());
            }
        }

        if !full {
            return Ok(true);
        }

        // Then, deserialise all of the inner maps.
        let threads = map
            .iter()
            .map(|(short_key, inner_map)| {
                let prefix = prefix.to_owned();
                let short_key = short_key.to_string();
                let inner_map = Arc::clone(inner_map);
                std::thread::spawn::<_, anyhow::Result<()>>(move || {
                    let mut inner_map = inner_map.write().unwrap();
                    let file = std::fs::File::open(prefix.join(short_key).with_extension("jsonl"))?;
                    for line in BufReader::new(file).lines() {
                        let line = line?;
                        if line.is_empty() {
                            continue;
                        }
                        let (key, value) = serde_json::from_str(&line)?;
                        inner_map.insert(key, value);
                    }
                    Ok(())
                })
            })
            .collect::<Vec<_>>();

        for thread in threads {
            thread.join().map_err(|_| anyhow::Error::msg("panic"))??;
        }

        self.mark_loaded();

        Ok(true)
    }
}

/// Performs a binary search on the given file to try to find the given key-value pair.
fn find_entry_in_file<L, V>(file: &mut File, key: &L) -> anyhow::Result<Option<V>>
where
    L: Ord + for<'a> Deserialize<'a>,
    V: for<'a> Deserialize<'a>,
{
    binary_search_line_in_file(
        file,
        |line| {
            let (key, _): (L, V) = serde_json::from_str(line).unwrap();
            key
        },
        key,
    )
    .map(|line| {
        line.map(|line| {
            let (_, value): (L, V) = serde_json::from_str(&line).unwrap();
            value
        })
    })
}
