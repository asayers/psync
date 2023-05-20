use anyhow::{anyhow, ensure};
use clap::Parser;
use kdam::{Bar, BarExt};
use sha2::Digest;
use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
};
use xorf::Filter;

#[derive(Parser)]
enum Cmd {
    Search {
        #[clap(long, short)]
        config: PathBuf,
        #[clap(long, short)]
        seed: PathBuf,
    },
    Chunk {
        path: PathBuf,
        #[clap(long, short, default_value = "65536")]
        size: usize,
    },
}

fn main() -> anyhow::Result<()> {
    match Cmd::parse() {
        Cmd::Search { config, seed } => search(config, seed),
        Cmd::Chunk { path, size } => chunk(path, size),
    }
}

fn chunk(path: PathBuf, size: usize) -> anyhow::Result<()> {
    println!("# from\tlength\tstart_mark\tsha-256");
    chunk_uniform(&path, size)
}

fn chunk_uniform(path: &Path, size: usize) -> anyhow::Result<()> {
    ensure!(size >= WINDOW_SIZE, "Chunk size too small");
    let file = File::open(path)?;
    let mmap = unsafe { memmap2::Mmap::map(&file)? };
    eprintln!(
        "Chunking a {} MiB file with a uniform chunk size of {} KiB: expecting {} chunks",
        mmap.len() / (1024 * 1024),
        size / 1024,
        1 + (mmap.len() - 1) / size,
    );
    let mut hasher = RollSum::default();
    let mut pb = mk_bar(mmap.len())?;
    for (i, &x) in (&mmap[..]).iter().enumerate() {
        hasher.input(x);
        if i >= WINDOW_SIZE - 1 {
            let from = i - WINDOW_SIZE + 1;
            if from % size == 0 {
                let start_mark = hasher.sum();
                let to = mmap.len().min(from + size);
                let hash = sha2::Sha256::digest(&mmap[from..to]);
                println!("{from}\t{size}\t{start_mark:x}\t{hash:x}");
                pb.update_to(i);
            }
        }
    }
    pb.refresh();
    eprintln!();
    Ok(())
}

fn mk_bar(total: usize) -> anyhow::Result<Bar> {
    Bar::builder()
        .total(total)
        .unit("B")
        .unit_scale(true)
        .ncols(70_i16)
        .build()
        .map_err(|e| anyhow!(e))
}

fn chunk_specific(path: PathBuf, from: usize, length: usize) -> anyhow::Result<()> {
    ensure!(length >= WINDOW_SIZE, "Chunk too short");
    let file = File::open(path)?;
    let mmap = unsafe { memmap2::Mmap::map(&file)? };
    let mut hasher = RollSum::default();
    for &x in &mmap[from..from + WINDOW_SIZE] {
        hasher.input(x);
    }
    let start_mark = hasher.sum();
    let hash = sha2::Sha256::digest(&mmap[from..from + length]);
    println!("# from\tlength\tstart_mark\tsha-256");
    println!("{from}\t{length}\t{start_mark:x}\t{hash:x}");
    Ok(())
}

type ChunkHash = [u8; 32];

struct Config {
    chunks: HashMap<u64, Vec<(usize, ChunkHash)>>,
    appearances: HashMap<ChunkHash, Vec<usize>>,
}

impl Config {
    fn read(path: &Path) -> anyhow::Result<Config> {
        let config = BufReader::new(File::open(path)?);
        let mut chunks: HashMap<u64, Vec<(usize, ChunkHash)>> = HashMap::default();
        let mut appearances: HashMap<ChunkHash, Vec<usize>> = HashMap::default();
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
            appearances_entry.or_default().push(from);
        }
        Ok(Config {
            chunks,
            appearances,
        })
    }

    fn n_chunks(&self) -> usize {
        self.chunks.values().map(|x| x.len()).sum()
    }

    fn n_appearances(&self) -> usize {
        self.appearances.values().map(|x| x.len()).sum()
    }
}

fn search(config: PathBuf, seed: PathBuf) -> anyhow::Result<()> {
    let config = Config::read(&config)?;
    let start_marks: Vec<u64> = config.chunks.keys().copied().collect();
    let filter = xorf::BinaryFuse32::try_from(&start_marks).map_err(|e| anyhow!(e))?;
    eprintln!(
        "Searching for {} chunks, appearing in {} positions",
        config.n_chunks(),
        config.n_appearances(),
    );

    let mut hasher = RollSum::default();
    let file = File::open(seed)?;
    let mmap = unsafe { memmap2::Mmap::map(&file)? };
    let mut pb = mk_bar(mmap.len())?;
    let mut our_appearances = HashMap::<ChunkHash, usize>::default();
    for (n_bytes, &x) in (&mmap[..]).iter().enumerate() {
        hasher.input(x);
        let hash = hasher.sum();
        if n_bytes % 1_000 == 0 {
            pb.update_to(n_bytes);
        }
        if filter.contains(&hash) {
            for (len, their_sha) in config.chunks.get(&hash).into_iter().flatten() {
                let our_start = n_bytes + 1 - WINDOW_SIZE;
                let end = mmap.len().min(our_start + len);
                let our_sha = sha2::Sha256::digest(&mmap[our_start..end]);
                if our_sha[..] == their_sha[..] {
                    our_appearances.insert(*their_sha, our_start);
                }
            }
        }
    }
    pb.refresh();
    eprintln!();
    eprintln!(
        "Discovered {}/{} chunks",
        our_appearances.len(),
        config.n_chunks(),
    );
    Ok(())
}

// Stolen from oll3/bita
pub struct RollSum {
    s1: u64,
    s2: u64,
    offset: usize,
    window: [u8; WINDOW_SIZE],
}

const CHAR_OFFSET: u64 = 63;
const WINDOW_SIZE: usize = 4 * 1024;

impl Default for RollSum {
    fn default() -> Self {
        Self {
            s1: WINDOW_SIZE as u64 * CHAR_OFFSET,
            s2: WINDOW_SIZE as u64 * (WINDOW_SIZE as u64 - 1) * CHAR_OFFSET,
            offset: 0,
            window: [0; WINDOW_SIZE],
        }
    }
}

impl RollSum {
    #[inline(always)]
    pub fn input(&mut self, in_val: u8) {
        let out_val = self.window[self.offset] as u64;
        {
            self.s1 = self.s1.wrapping_add(in_val as u64);
            self.s1 = self.s1.wrapping_sub(out_val);
            self.s2 = self.s2.wrapping_add(self.s1);
            self.s2 = self
                .s2
                .wrapping_sub((WINDOW_SIZE as u64) * (out_val + CHAR_OFFSET));
        }
        self.window[self.offset] = in_val;
        self.offset += 1;
        if self.offset >= WINDOW_SIZE {
            self.offset = 0;
        }
    }

    #[inline(always)]
    pub fn sum(&self) -> u64 {
        (self.s1 << 32) | (self.s2 & 0xffff_ffff)
    }
}
