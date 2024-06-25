use std::sync::Arc;

use crate::{
    page::{get_dump_status, page_stream},
    parse::wikitext::find_links,
    titles::id_to_title,
};

pub fn execute() -> anyhow::Result<()> {
    let id_to_title = Arc::new(id_to_title()?);

    /*
    let stream = page_stream(1, 1, "Scanning page links".to_owned(), move |page| {
        let id_to_title = id_to_title.clone();
        (
            page.id,
            find_links(page.revision.text)
                .iter()
                .map(|link| {
                    let id = id_to_title.get_id(link.target);
                    if id.is_none() {
                        eprintln!("Could not resolve Wikilink {} on page {}", link, page.title)
                    }
                    id
                })
                .collect::<Vec<_>>(),
        )
    })?;

    while let Ok((id, targets)) = stream.recv() {
        println!("Page {id} has {} link targets", targets.len());
    } */

    Ok(())
}
