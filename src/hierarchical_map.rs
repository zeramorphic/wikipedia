use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
    io::{BufWriter, Write},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
};

use serde::Serialize;

type LockedBTreeMap<K, V> = Arc<RwLock<BTreeMap<K, V>>>;

/// A nested map type, associating values of type `V` to keys of type `L`.
/// A "short key" of type `K` is derived from each key of type `L`,
/// and this "short key" is used to partition the main map into many smaller maps,
/// which can be locked and (de)serialised separately.
///
/// We use [`BTreeMap`] internally so that the serialised output is consistent between runs,
/// and is easy to inspect if something goes wrong.
pub struct HierarchicalMap<K, L, V> {
    /// Whether this hierarchical map has been fully loaded from disk.
    fully_loaded: Arc<AtomicBool>,

    #[allow(clippy::type_complexity)]
    shorten: Arc<Box<dyn Fn(&L) -> K + Send + Sync + 'static>>,
    map: LockedBTreeMap<K, LockedBTreeMap<L, V>>,
}

impl<K, L, V> Clone for HierarchicalMap<K, L, V> {
    fn clone(&self) -> Self {
        Self {
            fully_loaded: Arc::clone(&self.fully_loaded),
            shorten: Arc::clone(&self.shorten),
            map: Arc::clone(&self.map),
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
    pub fn new(shorten: impl Fn(&L) -> K + Send + Sync + 'static) -> Self {
        Self {
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

    /// Obtains the value associated to the given key, applies `f` to it, and returns the result.
    /// If the key was not found, this returns [`None`].
    pub fn with<T>(&self, key: &L, f: impl FnOnce(&V) -> T) -> Option<T>
    where
        K: Ord,
        L: Ord,
    {
        let result = self
            .map
            .read()
            .unwrap()
            .get(&(self.shorten)(key))
            .and_then(|inner_map| inner_map.read().unwrap().get(key).map(f));
        if let Some(result) = result {
            return Some(result);
        }
        if self.is_fully_loaded() {
            return None;
        }

        // Try to load this key-value pair from disk.
        todo!("try to load from disk");
    }

    /// Returns the underying map.
    pub fn get_map(&self) -> &LockedBTreeMap<K, LockedBTreeMap<L, V>> {
        &self.map
    }

    /// Serialises this hierarchical map using the given prefix, which should be something like `folder/information`.
    /// The output will be a file of the form `folder/information.json`, and a folder `folder/information/` which
    /// will contain a bz2 file for each short key used.
    pub fn serialize(&self, prefix: PathBuf) -> anyhow::Result<()>
    where
        K: Send + Sync + Serialize + Display,
        L: Send + Sync + Serialize + 'static,
        V: Send + Sync + Serialize + 'static,
    {
        if !self.is_fully_loaded() {
            panic!("hierarchical map not fully loaded before serialising");
        }

        let prefix = PathBuf::from("data").join(prefix);
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
                    let file = std::fs::File::create(prefix.join(short_key).with_extension("jsonl"))?;
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
}
