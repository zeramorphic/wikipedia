use super::xml::{make_errors_static, parse_element};

/// Parses a directory index that has been rendered to HTML, as in <https://dumps.wikimedia.org/enwiki/>.
pub fn file_names(html_index: &str) -> anyhow::Result<Vec<String>> {
    let (_, element) = make_errors_static(parse_element(html_index))?;

    // Get a list of all of the link hrefs that could point to directories.
    let mut hrefs = element
        .find("body")?
        .find("pre")?
        .children
        .iter()
        .map(|child| child.get_attribute("href").map(|value| value.to_owned()))
        .collect::<Result<Vec<_>, _>>()?;

    // Ignore the obvious paths that we'll never care about.
    hrefs.retain(|value| *value != "../" && *value != "./");

    Ok(hrefs)
}
