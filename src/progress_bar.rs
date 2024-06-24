use std::time::Duration;

use indicatif::{ProgressBar, ProgressStyle};

pub fn normal_progress_bar(len: u64) -> ProgressBar {
    let progress = ProgressBar::new(len);
    progress.set_style(
        ProgressStyle::with_template("{spinner:.green} {msg} {pos:.bold.bright}/{len:.bold.bright} [{elapsed_precise}] ({eta_precise})")
            .unwrap(),
    );
    progress.enable_steady_tick(Duration::from_millis(100));
    progress
}

pub fn file_progress_bar(len: u64) -> ProgressBar {
    let file_progress = ProgressBar::new(len);
    file_progress.set_style(ProgressStyle::with_template("{spinner:.green} {msg} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta_precise})")
        .unwrap()
        .progress_chars("#>-"));
    file_progress.enable_steady_tick(Duration::from_millis(100));
    file_progress
}
