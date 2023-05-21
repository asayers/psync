use crate::{rollsum::*, ChunkHash};
use anyhow::ensure;
use sha2::Digest;
use tracing::*;

pub type Appearance = (usize, usize, u64, ChunkHash);

pub fn chunk_uniform(
    file: &[u8],
    size: usize,
) -> anyhow::Result<impl Iterator<Item = Appearance> + '_> {
    ensure!(size >= WINDOW_SIZE, "Chunk size too small");
    info!(
        "Chunking a {} MiB file with a uniform chunk size of {} KiB: expecting {} chunks",
        file.len() / (1024 * 1024),
        size / 1024,
        1 + (file.len() - 1) / size,
    );
    let mut hasher = RollSum::default();
    Ok(file.iter().enumerate().filter_map(move |(i, &x)| {
        hasher.input(x);
        if let Some(from) = (i + 1).checked_sub(WINDOW_SIZE) {
            if from % size == 0 {
                let start_mark = hasher.sum();
                let to = file.len().min(from + size);
                let hash = sha2::Sha256::digest(&file[from..to]).try_into().unwrap();
                return Some((from, size, start_mark, hash));
            }
        }
        None
    }))
}

pub fn chunk_specific(file: &[u8], from: usize, length: usize) -> anyhow::Result<Appearance> {
    ensure!(length >= WINDOW_SIZE, "Chunk too short");
    let mut hasher = RollSum::default();
    for &x in &file[from..from + WINDOW_SIZE] {
        hasher.input(x);
    }
    let start_mark = hasher.sum();
    let hash = sha2::Sha256::digest(&file[from..from + length])
        .try_into()
        .unwrap();
    Ok((from, length, start_mark, hash))
}
