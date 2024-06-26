use std::collections::HashMap;

use console::style;

use crate::{
    hierarchical_map::HierarchicalMap,
    titles::{canonicalise_wikilink, generate_title_map},
};

use super::links::{generate_incoming_links, generate_outgoing_links};

pub fn execute(start: String, end: String) -> anyhow::Result<()> {
    let title_map = generate_title_map(false)?;
    let outgoing_links = generate_outgoing_links(false)?;
    let incoming_links = generate_incoming_links(false)?;

    let start = title_map.get_id(&canonicalise_wikilink(&start)).unwrap();
    let end = title_map.get_id(&canonicalise_wikilink(&end)).unwrap();

    let path = Solver::new(start, end).solve(&outgoing_links, &incoming_links, true);
    match path {
        Some(path) => {
            println!(
                "\nMinimal path of degree {} found!",
                style(path.len() - 1).bold().bright()
            );
            for (i, item) in path.iter().enumerate() {
                let title = title_map.get_title(*item).unwrap();
                if i == 0 {
                    println!("{} {}", style("start").red(), title);
                } else if i == path.len() - 1 {
                    println!("  {} {}", style("end").green(), title);
                } else {
                    println!("{:>5} {}", style(format!("{i}.")).dim(), title)
                }
            }
        }
        None => {
            println!("\nNo path exists.");
        }
    }

    Ok(())
}

pub struct Solver {
    /// The `n`th entry maps IDs `id` of "rank `n`" to IDs of "rank `n - 1`" that have a link to `id`.
    /// By convention, the `0`th entry consists of the single pair `(start, 0)` where `start` is the start article.
    /// Once `start` and `end` meet in the middle, we can use their data to reconstruct the full path.
    start: Vec<HashMap<u32, u32>>,
    /// The `n`th entry maps IDs `id` of "rank `n`" to IDs of "rank `n - 1`" that `id` links to.
    /// By convention, the `0`th entry consists of the single pair `(end, 0)` where `end` is the end article.
    /// Once `start` and `end` meet in the middle, we can use their data to reconstruct the full path.
    end: Vec<HashMap<u32, u32>>,
}

impl Solver {
    pub fn new(start: u32, end: u32) -> Self {
        Self {
            start: vec![{
                let mut result = HashMap::new();
                result.insert(start, 0);
                result
            }],
            end: vec![{
                let mut result = HashMap::new();
                result.insert(end, 0);
                result
            }],
        }
    }

    fn populate_forward(&mut self, outgoing_links: &HierarchicalMap<u8, u32, Vec<u32>>) {
        let mut new_map = HashMap::new();
        for id in self.start.last().unwrap().keys() {
            for link in outgoing_links
                .with(id, |links| links.clone())
                .into_iter()
                .flatten()
            {
                // Because of how we conduct the search, we don't need to re-add articles we've already looked at.
                if !self.start.iter().any(|map| map.contains_key(&link)) {
                    new_map.insert(link, *id);
                }
            }
        }
        self.start.push(new_map);
    }

    fn populate_backward(&mut self, incoming_links: &HierarchicalMap<u8, u32, Vec<u32>>) {
        let mut new_map = HashMap::new();
        for id in self.end.last().unwrap().keys() {
            for link in incoming_links
                .with(id, |links| links.clone())
                .into_iter()
                .flatten()
            {
                if !self.end.iter().any(|map| map.contains_key(&link)) {
                    new_map.insert(link, *id);
                }
                new_map.insert(link, *id);
            }
        }
        self.end.push(new_map);
    }

    /// Return a currently discovered complete path, if one exists.
    fn complete_path(&self) -> Option<Vec<u32>> {
        let start_map = self.start.last().unwrap();
        let end_map = self.end.last().unwrap();

        if let Some(connection) = start_map.keys().find(|key| end_map.contains_key(key)) {
            // We found a path.
            let mut path = Vec::new();
            let mut towards_start = *connection;
            let mut start_rank = self.start.len() - 1;
            // The two different conditions here protect against off-by-one errors.
            while towards_start != 0 {
                path.insert(0, towards_start);
                towards_start = self.start[start_rank][&towards_start];
                start_rank -= 1;
            }
            let mut towards_end = *connection;
            let mut end_rank = self.end.len() - 1;
            while end_rank != 0 {
                towards_end = self.end[end_rank][&towards_end];
                path.push(towards_end);
                end_rank -= 1;
            }
            Some(path)
        } else {
            None
        }
    }

    pub fn solve(
        mut self,
        outgoing_links: &HierarchicalMap<u8, u32, Vec<u32>>,
        incoming_links: &HierarchicalMap<u8, u32, Vec<u32>>,
        print_progress: bool,
    ) -> Option<Vec<u32>> {
        loop {
            if print_progress {
                println!(
                    "\n{}",
                    style(format!(
                        "= Stage {} =",
                        self.start.len() + self.end.len() - 1
                    ))
                    .bold()
                    .dim()
                );
                println!("Current depth {}-{}", self.start.len(), self.end.len());
                println!(
                    "Frontier size {}-{}",
                    self.start.last().unwrap().len(),
                    self.end.last().unwrap().len()
                );
            }

            if self.start.last().unwrap().is_empty() || self.end.last().unwrap().is_empty() {
                // We've exhausted all of the possibilities for one of the two directions,
                // so no path exists.
                return None;
            }

            if let Some(path) = self.complete_path() {
                return Some(path);
            }

            if self.start.last().unwrap().len() <= self.end.last().unwrap().len() {
                if print_progress {
                    println!("Populating forward");
                }
                self.populate_forward(outgoing_links);
            } else {
                if print_progress {
                    println!("Populating backward");
                }
                self.populate_backward(incoming_links);
            }
        }
    }
}
