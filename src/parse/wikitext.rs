use std::fmt::Display;

pub fn find_links(text: &str) -> Vec<Wikilink> {
    let mut output = Vec::new();
    for (start, _) in text.match_indices("[[") {
        if let Some(mut end) = text[start + 2..].find("]]") {
            end += start + 2;
            let contents = &text[start + 2..end];
            match contents.split_once('|') {
                Some((target, text)) => output.push(Wikilink { target, text }),
                None => output.push(Wikilink {
                    target: contents,
                    text: contents,
                }),
            }
        }
    }
    output
}

#[derive(Debug)]
pub struct Wikilink<'a> {
    pub target: &'a str,
    pub text: &'a str,
}

impl<'a> Display for Wikilink<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[[{}|{}]]", self.target, self.text)
    }
}
