use std::{fmt::Display, path::PathBuf};

use percent_encoding::percent_decode_str;

use crate::hierarchical_map::HierarchicalMap;

pub fn generate_title_map(full: bool) -> anyhow::Result<TitleMap> {
    let id_to_title = TitleMap::default();
    if !id_to_title.deserialise(full)? {
        // If we haven't already saved the title map to disk, we need to compute it in its entirety, then save it to disk.
        let rx =
            crate::page::page_stream(u64::MAX, 1, "Precomputing page IDs".to_owned(), |page| {
                (page.id, page.title.to_owned())
            })?;

        while let Ok((id, title)) = rx.recv() {
            id_to_title.insert(id, canonicalise_wikilink(&title));
        }

        id_to_title.mark_loaded();
        println!("{id_to_title}");
        id_to_title.serialise()?;
    }

    Ok(id_to_title)
}

#[derive(Debug, Clone)]
pub struct TitleMap {
    id_to_title: HierarchicalMap<u8, u32, String>,
    title_to_id: HierarchicalMap<String, String, u32>,
}

impl Default for TitleMap {
    fn default() -> Self {
        Self {
            id_to_title: HierarchicalMap::new(PathBuf::from("id_to_title"), id_short_key),
            title_to_id: HierarchicalMap::new(PathBuf::from("title_to_id"), |string: &String| {
                title_short_key(string)
            }),
        }
    }
}

impl Display for TitleMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "\n\nID TITLE MAP SUMMARY{}\n\n= ID to title =\n{}\n\n= Title to ID =\n{}",
            if !self.id_to_title.is_fully_loaded() || !self.title_to_id.is_fully_loaded() {
                " (incomplete)"
            } else {
                ""
            },
            self.id_to_title,
            self.title_to_id
        )
    }
}

/// A somewhat reasonable short key to use with [`HierarchicalMap`].
/// This is simply obtained from the least significant 8 bits of the ID.
///
/// This gives us a total of at most 256 short keys.
pub fn id_short_key(id: &u32) -> u8 {
    *id as u8
}

/// A somewhat reasonable short key to use with [`HierarchicalMap`].
/// We'll use the first (one or) two non-namespace characters as a key, as long as they are ASCII-alphabetic.
/// We make them uppercase for convenience.
/// If there aren't any ASCII alphabetic characters, we put it in a generic non-ASCII-alphabetic short key.
///
/// This gives a maximum of (26 * (26 + 1) + 1) = 703 short keys.
pub fn title_short_key(title: &str) -> String {
    // This will catch some false positives that aren't really namespaces, but this shouldn't matter for our use case.
    let title = match title.split_once(':') {
        Some((_, suffix)) => suffix,
        None => title,
    };

    let short_key = title
        .chars()
        .filter(|c| c.is_ascii_alphabetic())
        .map(|c| c.to_ascii_uppercase())
        .take(2)
        .collect::<String>();
    if short_key.is_empty() {
        "other".to_owned()
    } else {
        short_key
    }
}

impl TitleMap {
    pub fn get_title(&self, id: u32) -> Option<String> {
        self.id_to_title.with(&id, String::clone)
    }

    pub fn get_id(&self, title: &str) -> Option<u32> {
        self.title_to_id
            .with(&canonicalise_wikilink(title), u32::clone)
    }

    fn mark_loaded(&self) {
        self.id_to_title.mark_loaded();
        self.title_to_id.mark_loaded();
    }

    fn insert(&self, id: u32, title: String) {
        let title = canonicalise_wikilink(&title);
        self.id_to_title.insert(id, title.clone());
        self.title_to_id.insert(title, id);
    }

    fn serialise(&self) -> anyhow::Result<()> {
        self.id_to_title.serialize()?;
        self.title_to_id.serialize()?;
        Ok(())
    }

    fn deserialise(&self, full: bool) -> anyhow::Result<bool> {
        Ok(self.id_to_title.deserialize(full)? && self.title_to_id.deserialize(full)?)
    }
}

/// <https://en.wikipedia.org/wiki/Help:Link#Conversion_to_canonical_form>
pub fn canonicalise_wikilink(input: &str) -> String {
    let (namespace, input) = match input.split_once(':') {
        Some((namespace, remaining_input)) => {
            let namespace = match namespace.trim().to_lowercase().as_str() {
                "main" => Some("Main"),
                "article" => Some("Article"),
                "user" => Some("User"),
                "wikipedia" => Some("Wikipedia"),
                "file" => Some("File"),
                "mediawiki" => Some("MediaWiki"),
                "template" => Some("Template"),
                "help" => Some("Help"),
                "category" => Some("Category"),
                "portal" => Some("Portal"),
                "draft" => Some("Draft"),
                "timedtext" => Some("TimedText"),
                "module" => Some("Module"),
                "special" => Some("Special"),
                "media" => Some("Media"),
                _ => None,
            };
            match namespace {
                Some(namespace) => (Some(namespace), remaining_input),
                None => (None, input),
            }
        }
        None => (None, input),
    };

    let unescaped = String::from_utf8(percent_decode_str(input).collect::<Vec<_>>()).unwrap();
    let unescaped = html_escape::decode_html_entities(&unescaped);

    let title_case = unescaped
        .chars()
        .next()
        .unwrap()
        .to_uppercase()
        .chain(input.chars().skip(1))
        .collect::<String>();

    let no_underscores = title_case
        .replace("_", " ")
        .split(' ')
        .collect::<Vec<_>>()
        .join(" ");

    match namespace {
        Some(namespace) => format!("{namespace}:{no_underscores}"),
        None => no_underscores,
    }
}
