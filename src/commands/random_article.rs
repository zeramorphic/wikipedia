use crate::page::{count_articles, get_dump_status, id_to_title};

pub async fn execute() -> anyhow::Result<()> {
    let dump_status = get_dump_status().await?;
    let article_count = count_articles(&dump_status).await?;
    let id_to_title = id_to_title().await?;
    Ok(())
}
