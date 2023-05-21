use anyhow::anyhow;
use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

pub type ChunkHash = [u8; 32];

pub struct ControlFile {
    pub chunks: HashMap<u64, Vec<(usize, ChunkHash)>>,
    pub appearances: HashMap<ChunkHash, (usize, Vec<usize>)>,
}

impl ControlFile {
    pub fn read(path: &Path) -> anyhow::Result<ControlFile> {
        let config = BufReader::new(File::open(path)?);
        let mut chunks: HashMap<u64, Vec<(usize, ChunkHash)>> = HashMap::default();
        let mut appearances: HashMap<ChunkHash, (usize, Vec<usize>)> = HashMap::default();
        for l in config.lines() {
            let l = l?;
            if l.starts_with('#') {
                continue;
            }
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
