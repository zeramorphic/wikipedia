pub mod binary_search_line;
pub mod commands;
pub mod hierarchical_map;
pub mod memoise;
pub mod page;
pub mod parse;
pub mod progress_bar;
pub mod titles;

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Downloads Wikipedia data files
    Download {
        #[arg(short, long)]
        date: Option<String>,
    },
    /// Displays a random article
    Random {},
    /// Displays the list of articles linked from an article
    Links { article: String },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Download { date } => commands::download::execute(date),
        Commands::Random {} => commands::random_article::execute(),
        Commands::Links { article } => commands::links::execute(article),
    }
}
