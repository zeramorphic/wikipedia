use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use crate::{
    hierarchical_map::HierarchicalMap,
    page::page_stream,
    parse::wikitext::find_links,
    titles::{
        canonicalise_wikilink, generate_title_map, id_short_key, is_interwiki_link, split_namespace,
    },
};

use itertools::Itertools;

pub fn execute(article: String) -> anyhow::Result<()> {
    let title_map = generate_title_map(false)?;
    let outgoing_links = generate_outgoing_links(false)?;
    let incoming_links = generate_incoming_links(false)?;

    let id = title_map.get_id(&canonicalise_wikilink(&article)).unwrap();
    for link in outgoing_links.with(&id, |val| val.clone()).unwrap() {
        println!("> {}", title_map.get_title(link).unwrap());
    }
    for link in incoming_links.with(&id, |val| val.clone()).unwrap() {
        println!("< {}", title_map.get_title(link).unwrap());
    }

    Ok(())
}

pub fn generate_outgoing_links(full: bool) -> anyhow::Result<HierarchicalMap<u8, u32, Vec<u32>>> {
    let map = HierarchicalMap::new(PathBuf::from("outgoing_links"), id_short_key);
    if map.deserialize(full)? {
        return Ok(map);
    }

    let title_map = generate_title_map(true)?;

    let red_links = Arc::new(AtomicUsize::new(0));
    let red_links2 = red_links.clone();
    let stream = page_stream(
        u64::MAX,
        1,
        "Preprocessing outgoing links".to_string(),
        move |page| {
            (
                page.id,
                find_links(page.revision.text)
                    .into_iter()
                    .map(|link| link.target_root())
                    .filter(|root| {
                        let (namespace, root_remainder) = split_namespace(root);
                        let namespace_permitted =
                            matches!(namespace, None | Some("Category") | Some("Portal"));
                        namespace_permitted && !is_interwiki_link(root_remainder)
                    })
                    .filter_map(|root| match title_map.get_id(&root) {
                        Some(id) => Some(id),
                        None => {
                            red_links2.fetch_add(1, Ordering::SeqCst);
                            None
                        }
                    })
                    .unique()
                    .collect::<Vec<_>>(),
            )
        },
    )?;

    let mut blue_links = 0;
    for (page, links) in stream {
        blue_links += links.len();
        map.insert(page, links);
    }

    println!(
        "Finished preprocessing, found {blue_links} blue links and {} red links",
        red_links.load(Ordering::SeqCst)
    );

    map.mark_loaded();
    map.serialize()?;

    Ok(map)
}

pub fn generate_incoming_links(full: bool) -> anyhow::Result<HierarchicalMap<u8, u32, Vec<u32>>> {
    let map = HierarchicalMap::new(PathBuf::from("incoming_links"), id_short_key);
    if map.deserialize(full)? {
        return Ok(map);
    }

    let outgoing_links = generate_outgoing_links(true)?;
    let rx = outgoing_links.with_all("Preprocessing incoming links".to_owned(), |id, links| {
        (*id, links.to_owned())
    });
    while let Ok((id, links)) = rx.recv() {
        for link in links {
            map.mutate_with_default(link, |list| list.push(id));
        }
    }

    map.mark_loaded();
    map.serialize()?;

    Ok(map)
}
