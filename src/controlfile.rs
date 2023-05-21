use anyhow::anyhow;
use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};
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
            let mut fields = l.split_ascii_whitespace();
            let mut next_field = || fields.next().ok_or_else(|| anyhow!("Not enough fields"));
            let from: usize = next_field()?.parse()?;
            let length: usize = next_field()?.parse()?;
            let start_mark = u64::from_str_radix(next_field()?, 16)?;
            let hash = hex::decode(next_field()?)?.try_into().unwrap();
            let appearances_entry = appearances.entry(hash);
            if matches!(
                appearances_entry,
                std::collections::hash_map::Entry::Vacant(_)
            ) {
                chunks.entry(start_mark).or_default().push((length, hash));
            }
            appearances_entry
                .or_insert_with(|| (length, vec![]))
                .1
                .push(from);
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
