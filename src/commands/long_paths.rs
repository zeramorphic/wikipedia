use std::io::Write;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use console::style;

use crate::{
    commands::{random_article::random_article_id, shortest_path},
    page::get_dump_status,
    titles::generate_title_map,
};

use super::links::{generate_incoming_links, generate_outgoing_links};

pub fn execute() -> anyhow::Result<()> {
    let dump_status = get_dump_status()?;
    println!("Loading title map");
    let title_map = generate_title_map(true)?;
    println!("Loading outgoing link map");
    let outgoing_links = generate_outgoing_links(true)?;
    println!("Loading incoming link map");
    let incoming_links = generate_incoming_links(true)?;
    println!("All data loaded.");

    let longest_path_length = Arc::new(AtomicUsize::new(0));
    let paths_tried = Arc::new(AtomicUsize::new(0));
    let tasks = (0..16)
        .map(|_| {
            let dump_status = dump_status.clone();
            let title_map = title_map.clone();
            let outgoing_links = outgoing_links.clone();
            let incoming_links = incoming_links.clone();

            let longest_path_length = longest_path_length.clone();
            let paths_tried = paths_tried.clone();
            std::thread::spawn::<_, anyhow::Result<()>>(move || {
                loop {
                    // A very simple algorithm to find some long paths: randomly select a pair of articles
                    // and compute the shortest distance between them.
                    let start = random_article_id(&dump_status, &title_map, true)?;
                    let end = random_article_id(&dump_status, &title_map, true)?;
                    let path = shortest_path::Solver::new(start, end).solve(
                        &outgoing_links,
                        &incoming_links,
                        false,
                    );
                    let paths_tried = paths_tried.fetch_add(1, Ordering::SeqCst);
                    if paths_tried % 100 == 0 {
                        println!("Tried {paths_tried} paths");
                    }
                    if let Some(path) = path {
                        if path.len() >= longest_path_length.load(Ordering::SeqCst) {
                            longest_path_length.fetch_max(path.len(), Ordering::SeqCst);

                            let mut out = std::io::stdout().lock();
                            writeln!(
                                out,
                                "\nMinimal path of degree {} found!",
                                style(path.len() - 1).bold().bright()
                            )?;
                            for (i, item) in path.iter().enumerate() {
                                let title = title_map.get_title(*item).unwrap();
                                if i == 0 {
                                    writeln!(out, "{} {}", style("start").red(), title)?;
                                } else if i == path.len() - 1 {
                                    writeln!(out, "  {} {}", style("end").green(), title)?;
                                } else {
                                    writeln!(out, "{:>5} {}", style(format!("{i}.")).dim(), title)?;
                                }
                            }
                            writeln!(out)?;
                        }
                    }
                }
            })
        })
        .collect::<Vec<_>>();

    for task in tasks {
        task.join().map_err(|_| anyhow::Error::msg("panic"))??;
    }

    Ok(())
}
