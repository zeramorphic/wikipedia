pub mod commands;
pub mod page;
pub mod parse;
pub mod progress_bar;
pub mod memoise;

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Downloads Wikipedia data files
    Download {},
    /// Displays a random article
    Random {},
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Download {} => commands::download::execute().await,
        Commands::Random {} => commands::random_article::execute().await,
    }
}
