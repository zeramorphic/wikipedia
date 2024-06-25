use rand::Rng;

use crate::{
    page::{get_dump_status, page_information},
    titles::generate_title_map,
};

pub fn execute() -> anyhow::Result<()> {
    let dump_status = get_dump_status()?;
    let title_map = generate_title_map(false)?;

    let random_id = loop {
        let random_id = rand::thread_rng().gen_range(0..100_000_000u32);
        println!("Trying ID {random_id}");
        match title_map.get_title(random_id) {
            Some(title) => {
                println!("Found article {title}");
                let is_redirect =
                    page_information(&dump_status, 20324344, |page| page.redirect.is_some())?;
                if is_redirect {
                    println!("This article is a redirect; ignoring");
                } else {
                    break random_id;
                }
            }
            None => {
                println!("ID {random_id} was not an article");
            }
        }
    };

    let title = title_map.get_title(random_id).unwrap();
    println!("Generated random article with id {random_id} and title {title}",);

    Ok(())
}
