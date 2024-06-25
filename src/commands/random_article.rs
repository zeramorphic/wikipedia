use std::sync::Arc;

use crate::{
    page::{
        canonicalise_wikilink, count_articles, get_dump_status, id_to_title, page_information,
        page_stream,
    },
    parse::wikitext::find_links,
};

pub async fn execute() -> anyhow::Result<()> {
    let dump_status = get_dump_status().await?;
    // let article_count = count_articles(&dump_status).await?;
    let id_to_title = Arc::new(id_to_title().await?);

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
    })
    .await?;

    while let Ok((id, targets)) = stream.recv().await {
        println!("Page {id} has {} link targets", targets.len());
    }

    Ok(())
}
