use crate::{rollsum::*, Sha256Sum};
use anyhow::ensure;
use sha2::Digest;
use tracing::*;

pub type Appearance = (usize, usize, u64, Sha256Sum);

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

pub fn chunk_specific(file: &[u8], mut from: usize, mut length: usize) -> Appearance {
    if from + WINDOW_SIZE >= file.len() {
        from = file.len() - WINDOW_SIZE;
        length = WINDOW_SIZE;
        debug!("Chunk is too close to the EOF.  Shifting back to {}", from);
    }
    if from + length > file.len() {
        length = file.len() - from;
        debug!(
            "Chunk extends beyond EOF, truncating to {} KiB",
            length / 1024
        );
    }
    if length < WINDOW_SIZE {
        debug!(
            "Very small chunk requested: {} KiB.  Expanding to {} KiB",
            length / 1024,
            WINDOW_SIZE / 1024
        );
        length = WINDOW_SIZE;
    }

    let mut hasher = RollSum::default();
    for &x in &file[from..from + WINDOW_SIZE] {
        hasher.input(x);
    }
    let start_mark = hasher.sum();
    let hash = sha2::Sha256::digest(&file[from..from + length])
        .try_into()
        .unwrap();
    (from, length, start_mark, hash)
}

pub fn chunk_tarball(file: &[u8]) -> impl Iterator<Item = Appearance> + '_ {
    let mut offset = 0;
    info!("file len: {}", file.len());
    std::iter::from_fn(move || {
        if offset + 512 >= file.len() {
            return None;
        }
        let hdr = tar::Header::from_byte_slice(&file[offset..offset + 512]);
        let data_len = hdr.entry_size().ok()? as usize;
        let path = hdr.path().ok()?;
        let filename = path.file_name()?.to_str()?;
        let _g = info_span!("", %filename).entered();
        let x = ((data_len - 1) / 512) + 1; // round to 512 bytes
        let entry_len = (x + 1) * 512; // add 512 for the header
        let chunk = chunk_specific(file, offset, entry_len);
        offset += entry_len;
        Some(chunk)
    })
}
