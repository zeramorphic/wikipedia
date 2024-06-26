use rand::Rng;

use crate::{
    page::{get_dump_status, page_information},
    titles::{generate_title_map, split_namespace, TitleMap},
};

use super::download::DumpStatus;

pub fn execute() -> anyhow::Result<()> {
    let dump_status = get_dump_status()?;
    let title_map = generate_title_map(false)?;

    let id = random_article_id(&dump_status, &title_map, true)?;
    println!("Chosen random article {}", title_map.get_title(id).unwrap());

    Ok(())
}

/// If `root_namespace` is true, we return only articles in the root namespace.
pub fn random_article_id(
    dump_status: &DumpStatus,
    title_map: &TitleMap,
    root_namespace: bool,
) -> anyhow::Result<u32> {
    loop {
        let random_id = rand::thread_rng().gen_range(0..100_000_000u32);
        if let Some(title) = title_map.get_title(random_id) {
            let is_redirect =
                page_information(dump_status, random_id, |page| page.redirect.is_some())?;
            let (namespace, _) = split_namespace(&title);
            if !is_redirect && (!root_namespace || namespace.is_none()) {
                break Ok(random_id);
            }
        }
    }
}
