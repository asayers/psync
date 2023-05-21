use anyhow::anyhow;
use clap::Parser;
use kdam::{Bar, BarExt};
use psync::*;
use rangemap::RangeMap;
use sha2::Digest;
use std::{fs::File, path::PathBuf};
use tracing::*;

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
        #[clap(long, short, default_value = "65536", group = "chunker")]
        size: usize,
        /// Treat input as a tarball and chunk on entry boundaries
        #[clap(long, short, group = "chunker")]
        tar: bool,
    },
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();
    match Cmd::parse() {
        Cmd::Search { config, seed } => search(config, seed),
        Cmd::Chunk { path, size, tar } => chunk(path, size, tar),
    }
}

fn chunk(path: PathBuf, size: usize, tar: bool) -> anyhow::Result<()> {
    let file = File::open(path)?;
    let mmap = unsafe { memmap2::Mmap::map(&file)? };
    println!("# This file was created by psync");
    println!("# These fields relate to the file as a whole");
    println!("Length: {}", mmap.len());
    println!("SHA-256: {}", hex::encode(sha2::Sha256::digest(&mmap[..])));
    println!("---");
    println!("# These relate to chunks of the file");
    println!("# from\tlength\tstart_mark\tsha-256");
    let mut pb = mk_bar(mmap.len())?;
    let chunks: Box<dyn Iterator<Item = (usize, usize, u64, [u8; 32])>> = if tar {
        Box::new(chunkers::chunk_tarball(&mmap[..]))
    } else {
        Box::new(chunkers::chunk_uniform(&mmap[..], size)?)
    };
    for (from, size, start_mark, hash) in chunks {
        println!("{from}\t{size}\t{start_mark:x}\t{}", hex::encode(hash));
        pb.update_to(from);
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

fn search(control_file: PathBuf, seed: PathBuf) -> anyhow::Result<()> {
    let control_file = ControlFile::read(&control_file)?;
    info!(
        "Searching for {} chunks, appearing in {} positions",
        control_file.n_chunks(),
        control_file.n_appearances(),
    );

    let file = File::open(seed)?;
    let mmap = unsafe { memmap2::Mmap::map(&file)? };
    let mut pb = mk_bar(mmap.len())?;
    let our_appearances = psync::search(&mmap[..], &control_file, |n| {
        pb.update_to(n);
    })?;
    pb.refresh();
    eprintln!();
    info!(
        "Discovered {}/{} chunks",
        our_appearances.len(),
        control_file.n_chunks(),
    );

    // Maps ranges in their file to ranges in ours
    let mut coverage = RangeMap::<usize, isize /* offset */>::default();
    for (hash, our_start) in our_appearances {
        let (length, their_starts) = &control_file.appearances[&hash];
        for &their_start in their_starts {
            let offset = our_start as isize - their_start as isize;
            coverage.insert(their_start..their_start + *length, offset);
        }
    }
    for (theirs, offset) in coverage.iter() {
        info!("REUSABLE: {}..{}: {offset}", theirs.start, theirs.end);
    }
    for gap in coverage.gaps(&(0..control_file.total_len)) {
        info!("MISSING: {}..{}", gap.start, gap.end);
    }

    Ok(())
}
