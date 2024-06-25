use std::{borrow::Cow, fmt::Display};

use crate::titles::canonicalise_wikilink;

/// Finds a list of all links in this wikitext file.
/// This doesn't process nested links well, possibly giving shorter-than-expected `text`,
/// but will always give the correct `target`.
pub fn find_links(text: &str) -> Vec<Wikilink> {
    let mut output = Vec::new();
    for (start, _) in text.match_indices("[[") {
        if let Some(mut end) = text[start + 2..].find("]]") {
            end += start + 2;
            let contents = &text[start + 2..end];
            match contents.split_once('|') {
                Some((target, text)) => output.push(Wikilink {
                    target: Cow::Borrowed(target),
                    text: Cow::Borrowed(text),
                }),
                None => output.push(Wikilink {
                    target: Cow::Borrowed(contents),
                    text: Cow::Borrowed(contents),
                }),
            }
        }
    }
    output
}

#[derive(Debug)]
pub struct Wikilink<'a> {
    pub target: Cow<'a, str>,
    pub text: Cow<'a, str>,
}

impl<'a> Display for Wikilink<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[[{}|{}]]", self.target, self.text)
    }
}

impl<'a> Wikilink<'a> {
    pub fn to_owned(self) -> Wikilink<'static> {
        Wikilink {
            target: self.target.into_owned().into(),
            text: self.text.into_owned().into(),
        }
    }

    /// Gets the target, without any anchors indicated by `#`, then canonicalised.
    pub fn target_root(&self) -> String {
        match self.target.split_once('#') {
            Some((left, _)) => canonicalise_wikilink(left),
            None => canonicalise_wikilink(&self.target),
        }
    }
}
