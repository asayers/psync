use crate::{rollsum::*, Sha256Sum};
use anyhow::{anyhow, ensure};
use sha2::Digest;
use std::{fmt, str::FromStr};
use tracing::*;

pub struct Appearance {
    pub from: usize,
    pub len: usize,
    pub start_mark: u64,
    pub hash: Sha256Sum,
}

impl fmt::Display for Appearance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Appearance {
            from,
            len,
            start_mark,
            hash,
        } = self;
        write!(f, "{from}\t{len}\t{start_mark:x}\t{}", hex::encode(hash))
    }
}

impl FromStr for Appearance {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> anyhow::Result<Self> {
        let mut fields = s.split_ascii_whitespace();
        let mut next_field = || fields.next().ok_or_else(|| anyhow!("Not enough fields"));
        let from: usize = next_field()?.parse()?;
        let len: usize = next_field()?.parse()?;
        let start_mark = u64::from_str_radix(next_field()?, 16)?;
        let hash = hex::decode(next_field()?)?.try_into().unwrap();
        Ok(Appearance {
            from,
            len,
            start_mark,
            hash,
        })
    }
}

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
                return Some(Appearance {
                    from,
                    len: size,
                    start_mark,
                    hash,
                });
            }
        }
        None
    }))
}

pub fn chunk_specific(file: &[u8], mut from: usize, mut len: usize) -> Appearance {
    if from + WINDOW_SIZE >= file.len() {
        from = file.len() - WINDOW_SIZE;
        len = WINDOW_SIZE;
        debug!("Chunk is too close to the EOF.  Shifting back to {}", from);
    }
    if from + len > file.len() {
        len = file.len() - from;
        debug!("Chunk extends beyond EOF, truncating to {} KiB", len / 1024);
    }
    if len < WINDOW_SIZE {
        debug!(
            "Very small chunk requested: {} KiB.  Expanding to {} KiB",
            len / 1024,
            WINDOW_SIZE / 1024
        );
        len = WINDOW_SIZE;
    }

    let mut hasher = RollSum::default();
    for &x in &file[from..from + WINDOW_SIZE] {
        hasher.input(x);
    }
    let start_mark = hasher.sum();
    let hash = sha2::Sha256::digest(&file[from..from + len])
        .try_into()
        .unwrap();
    Appearance {
        from,
        len,
        start_mark,
        hash,
    }
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
