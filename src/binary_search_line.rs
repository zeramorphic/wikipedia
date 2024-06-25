use std::{
    fs::File,
    io::{BufRead, BufReader, Seek},
};

/// Returns the next complete line in the given file starting at the given byte offset.
fn next_line_starting_at(file: &mut File, start: u64) -> anyhow::Result<Option<String>> {
    file.seek(std::io::SeekFrom::Start(start))?;
    // We'll use a very small capacity because lines are short.
    let mut reader = BufReader::with_capacity(0x200, file);

    // If start > 0, skip the first line, because it could be incomplete.
    if start > 0 {
        let mut buf = Vec::new();
        reader.read_until(b'\n', &mut buf)?;
    }
    // Now read a full line.
    let mut buf = Vec::new();
    reader.read_until(b'\n', &mut buf)?;

    Ok(Some(String::from_utf8(buf)?))
}

/// Assume that `file` is a sequence of lines, such that applying `f` to each line in turn
/// produces an increasing sequence. Then return the line that matches the given key, or [`None`]
/// if one does not exist.
///
/// Ignores empty lines.
pub fn binary_search_line_in_file<L>(
    file: &mut File,
    get_key: impl Fn(&str) -> L,
    key: &L,
) -> anyhow::Result<Option<String>>
where
    L: Ord,
{
    let mut guess_min = 0u64;
    let mut guess_max = file.metadata()?.len();

    loop {
        // If the difference between `guess_max` and `guess_min` is two or less,
        // there is only one possible line we could obtain by guessing.
        let one_option = guess_max - guess_min <= 2;

        let guess = if one_option {
            // This makes sure that the entry at the start of the file is correctly read.
            guess_min
        } else {
            (guess_min + guess_max) / 2
        };

        let next_line = match next_line_starting_at(file, guess)? {
            Some(next_line) if !next_line.is_empty() => next_line,
            _ => {
                // We're too late into the file to have a next line.
                // The simplest solution is just to decrement `guess_max` by two, so that `guess` decrements by one.
                guess_max = guess_max.saturating_sub(2);
                continue;
            }
        };
        let found_key = get_key(&next_line);

        match key.cmp(&found_key) {
            std::cmp::Ordering::Less => {
                guess_max = guess;
            }
            std::cmp::Ordering::Equal => return Ok(Some(next_line)),
            std::cmp::Ordering::Greater => {
                guess_min = guess;
            }
        }

        if one_option {
            // We didn't find the result even though there was only one possible answer.
            return Ok(None);
        }
    }
}
