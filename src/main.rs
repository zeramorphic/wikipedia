pub mod commands;
pub mod memoise;
pub mod page;
pub mod parse;
pub mod progress_bar;
pub mod hierarchical_map;
pub mod titles;
pub mod binary_search_line;

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
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Download { date } => commands::download::execute(date),
        Commands::Random {} => commands::random_article::execute(),
    }
}
