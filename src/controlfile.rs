use crate::rollsum::*;
use anyhow::anyhow;
use sha2::Digest;
use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};
use std::{fmt, str::FromStr};
use tracing::*;

pub type Sha256Sum = [u8; 32];

pub struct ControlFile {
    pub total_len: usize,
    pub total_sha256: Sha256Sum,
    pub chunks: HashMap<u64, Vec<(usize, Sha256Sum)>>,
    pub appearances: HashMap<Sha256Sum, (usize, Vec<usize>)>,
}

impl ControlFile {
    pub fn read(path: &Path) -> anyhow::Result<ControlFile> {
        let mut chunks: HashMap<u64, Vec<(usize, Sha256Sum)>> = HashMap::default();
        let mut appearances: HashMap<Sha256Sum, (usize, Vec<usize>)> = HashMap::default();
        let mut total_len = None;
        let mut total_sha256 = None;

        let config = BufReader::new(File::open(path)?);
        let mut lines = config
            .lines()
            .map(|l| l.unwrap())
            .filter(|l| !l.starts_with('#'));
        for l in lines.by_ref() {
            let l = l.trim();
            if l == "---" {
                break;
            }
            match l.split_once(':') {
                None => warn!("{l}: Expected \"key: value\" pairs"),
                Some((k, v)) => {
                    let k = k.trim();
                    let v = v.trim();
                    match k {
                        "Length" => total_len = Some(v.parse()?),
                        "SHA-256" => total_sha256 = Some(hex::decode(v)?.try_into().unwrap()),
                        _ => warn!("{k}: Unrecognised header"),
                    }
                }
            }
        }
        for l in lines {
            let ap: Appearance = l.parse()?;
            let appearances_entry = appearances.entry(ap.hash);
            if matches!(
                appearances_entry,
                std::collections::hash_map::Entry::Vacant(_)
            ) {
                chunks
                    .entry(ap.start_mark)
                    .or_default()
                    .push((ap.len, ap.hash));
            }
            appearances_entry
                .or_insert_with(|| (ap.len, vec![]))
                .1
                .push(ap.from);
        }
        Ok(ControlFile {
            total_len: total_len.ok_or_else(|| anyhow!("Missing key: Length"))?,
            total_sha256: total_sha256.ok_or_else(|| anyhow!("Missing key: SHA-256"))?,
            chunks,
            appearances,
        })
    }

    pub fn n_chunks(&self) -> usize {
        self.chunks.values().map(|x| x.len()).sum()
    }

    pub fn n_appearances(&self) -> usize {
        self.appearances.values().map(|x| x.1.len()).sum()
    }

    pub fn mk_filter(&self) -> anyhow::Result<xorf::BinaryFuse32> {
        let start_marks: Vec<u64> = self.chunks.keys().copied().collect();
        xorf::BinaryFuse32::try_from(&start_marks).map_err(|e| anyhow!(e))
    }
}

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

impl Appearance {
    pub fn new(file: &[u8], mut from: usize, mut len: usize) -> Appearance {
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

        let mut roll_sum = RollSum::default();
        for &x in &file[from..from + WINDOW_SIZE] {
            roll_sum.input(x);
        }
        let start_mark = roll_sum.sum();
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
}
