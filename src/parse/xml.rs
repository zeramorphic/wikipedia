use nom::{
    bytes::complete::{tag, take_while, take_while1},
    IResult,
};

#[derive(Debug)]
pub struct Element<'a> {
    pub name: &'a str,
    pub attributes: Vec<(&'a str, &'a str)>,
    pub children: Vec<Element<'a>>,
    pub text: &'a str,
}

pub fn shorten(text: String) -> String {
    if text.len() < 100 {
        format!("{:?}", text)
    } else {
        let shorter = text.chars().take(80).collect::<String>();
        format!(
            "{:?}... ({} bytes hidden)",
            shorter,
            text.len() - shorter.len()
        )
    }
}

impl<'a> Element<'a> {
    pub fn summarise(&self) -> String {
        format!(
            "{}\n- attrs: {:?}\n- children: {:?}\n- text: {}",
            shorten(self.name.to_owned()),
            self.attributes
                .iter()
                .map(|(k, v)| (k, shorten((*v).to_owned())))
                .collect::<Vec<_>>(),
            self.children
                .iter()
                .map(|child| child.name)
                .collect::<Vec<_>>(),
            shorten(self.text.to_owned()),
        )
    }

    /// Finds the first child element with the given name.
    /// Raises an error if one does not exist.
    pub fn find(&self, name: &str) -> anyhow::Result<&Self> {
        self.children
            .iter()
            .find(|child| child.name == name)
            .ok_or_else(|| anyhow::Error::msg(format!("child with name {name} did not exist")))
    }

    pub fn get_attribute(&self, name: &str) -> anyhow::Result<&'a str> {
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

pub fn make_errors_static<T>(
    result: IResult<&str, T>,
) -> Result<(&str, T), nom::Err<nom::error::Error<String>>> {
    result.map_err(|err| {
        err.map(|err| nom::error::Error {
            input: err.input.to_owned(),
            code: err.code,
        })
    })
}

pub fn parse_whitespace(input: &str) -> IResult<&str, ()> {
    let (input, _) = take_while(|c: char| c.is_whitespace())(input)?;
    Ok((input, ()))
}

fn parse_attribute(input: &str) -> IResult<&str, (&str, &str)> {
    let (input, key) = take_while1(|c: char| !c.is_whitespace() && c != '=')(input)?;
    let (input, ()) = parse_whitespace(input)?;
    let (input, _) = tag("=")(input)?;
    let (input, ()) = parse_whitespace(input)?;
    let (input, _) = tag("\"")(input)?;
    let (input, value) = take_while(|c: char| c != '"')(input)?;
    let (input, _) = tag("\"")(input)?;
    Ok((input, (key, value)))
}

/// Returns `true` if this element is auto-closed.
fn parse_open_tag(input: &str) -> IResult<&str, (Element, bool)> {
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

    if let Some(input) = input.strip_prefix("/>") {
        Ok((input, (element, true)))
    } else {
        let (input, _) = tag(">")(input)?;
        Ok((input, (element, false)))
    }
}

fn parse_close_tag(input: &str) -> IResult<&str, &str> {
    let (input, _) = tag("</")(input)?;
    let (input, name) = take_while1(|c: char| c.is_ascii_alphanumeric())(input)?;
    let (input, _) = tag(">")(input)?;
    Ok((input, name))
}

pub fn parse_element(input: &str) -> IResult<&str, Element> {
    let (input, (mut element, auto_closed)) = parse_open_tag(input)?;

    if auto_closed {
        return Ok((input, element));
    }

    if element.name == "hr" {
        return Ok((input, element));
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
