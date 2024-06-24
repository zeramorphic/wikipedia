use nom::{
    bytes::complete::{tag, take_while, take_while1},
    IResult,
};

/// Parses a directory index that has been rendered to HTML, as in <https://dumps.wikimedia.org/enwiki/>.
pub fn file_names(html_index: &str) -> anyhow::Result<Vec<String>> {
    let (_, element) = parse_element(html_index).map_err(|err| {
        err.map(|err| nom::error::Error {
            input: err.input.to_owned(),
            code: err.code,
        })
    })?;

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

#[derive(Debug)]
pub struct Element<'a> {
    name: &'a str,
    attributes: Vec<(&'a str, &'a str)>,
    children: Vec<Element<'a>>,
    text: &'a str,
}

impl<'a> Element<'a> {
    /// Finds the first child element with the given name.
    /// Raises an error if one does not exist.
    pub fn find(&self, name: &str) -> anyhow::Result<&Self> {
        self.children
            .iter()
            .find(|child| child.name == name)
            .ok_or_else(|| anyhow::Error::msg(format!("child with name {name} did not exist")))
    }

    pub fn get_attribute(&self, name: &str) -> anyhow::Result<&str> {
        self.attributes
            .iter()
            .find_map(|(actual_name, value)| {
                if name == *actual_name {
                    Some(*value)
                } else {
                    None
                }
            })
            .ok_or_else(|| anyhow::Error::msg(format!("attribute with name {name} did not exist")))
    }
}

fn parse_whitespace(input: &str) -> IResult<&str, ()> {
    let (input, _) = take_while(|c: char| c.is_whitespace())(input)?;
    Ok((input, ()))
}

fn parse_attribute(input: &str) -> IResult<&str, (&str, &str)> {
    let (input, key) = take_while1(|c: char| c.is_ascii_alphanumeric())(input)?;
    let (input, ()) = parse_whitespace(input)?;
    let (input, _) = tag("=")(input)?;
    let (input, ()) = parse_whitespace(input)?;
    let (input, _) = tag("\"")(input)?;
    let (input, value) = take_while(|c: char| c != '"')(input)?;
    let (input, _) = tag("\"")(input)?;
    Ok((input, (key, value)))
}

fn parse_open_tag(input: &str) -> IResult<&str, Element> {
    let (input, _) = tag("<")(input)?;
    let (input, name) = take_while1(|c: char| c.is_ascii_alphanumeric())(input)?;
    let (mut input, ()) = parse_whitespace(input)?;

    let mut element = Element {
        name,
        attributes: Vec::new(),
        children: Vec::new(),
        text: "",
    };

    while input
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphanumeric())
    {
        let (new_input, attribute) = parse_attribute(input)?;
        let (new_input, ()) = parse_whitespace(new_input)?;
        element.attributes.push(attribute);
        input = new_input;
    }

    let (input, _) = tag(">")(input)?;
    Ok((input, element))
}

fn parse_close_tag(input: &str) -> IResult<&str, &str> {
    let (input, _) = tag("</")(input)?;
    let (input, name) = take_while1(|c: char| c.is_ascii_alphanumeric())(input)?;
    let (input, _) = tag(">")(input)?;
    Ok((input, name))
}

fn parse_element(input: &str) -> IResult<&str, Element> {
    let (input, mut element) = parse_open_tag(input)?;

    match element.name {
        "hr" => {
            return Ok((input, element));
        }
        _ => {}
    }

    let (input, text) = take_while(|c: char| c != '<')(input)?;
    element.text = text;

    let (mut input, ()) = parse_whitespace(input)?;

    while !input.is_empty() && !input.starts_with("</") {
        let (new_input, new_element) = parse_element(input)?;
        // This discards any additional text blocks.
        let (new_input, _) = take_while(|c: char| c != '<')(new_input)?;
        element.children.push(new_element);
        input = new_input;
    }

    let (close_input, close_name) = parse_close_tag(input)?;
    if element.name == close_name {
        Ok((close_input, element))
    } else {
        // Propagate the close outwards; this element is implicitly closed.
        Ok((input, element))
    }
}
