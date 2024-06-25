use rand::Rng;

use crate::{
    page::{get_dump_status, page_information},
    titles::{generate_title_map, TitleMap},
};

use super::download::DumpStatus;

pub fn execute() -> anyhow::Result<()> {
    let dump_status = get_dump_status()?;
    let title_map = generate_title_map(false)?;

    let id = random_article_id(&dump_status, &title_map)?;
    println!("Chosen random article {}", title_map.get_title(id).unwrap());

    Ok(())
}

fn random_article_id(dump_status: &DumpStatus, title_map: &TitleMap) -> anyhow::Result<u32> {
    loop {
        let random_id = rand::thread_rng().gen_range(0..100_000_000u32);
        if title_map.get_title(random_id).is_some() {
            let is_redirect =
                page_information(dump_status, 20324344, |page| page.redirect.is_some())?;
            if !is_redirect {
                break Ok(random_id);
            }
        }
    }
}
