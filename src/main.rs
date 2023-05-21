use anyhow::{anyhow, bail};
use clap::Parser;
use kdam::{Bar, BarExt};
use psync::*;
use rangemap::RangeMap;
use sha2::{Digest, Sha256};
use std::{fs::File, io::Write, path::PathBuf};
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
        /// Chunks larger than this will be split up
        #[clap(long, short, default_value = "65536")]
        max_size: usize,
        /// Treat input as a tarball and chunk on entry boundaries
        #[clap(long, short)]
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
        Cmd::Chunk {
            path,
            max_size,
            tar,
        } => chunk(path, max_size, tar),
    }
}

fn chunk(path: PathBuf, max_size: usize, tar: bool) -> anyhow::Result<()> {
    let file = File::open(&path)?;
    let mmap = unsafe { memmap2::Mmap::map(&file)? };
    let outpath = format!("{}.psync", path.display());
    let mut outfile = match File::options().write(true).create_new(true).open(&outpath) {
        Ok(x) => x,
        Err(_) => {
            bail!(
                "Control file at {outpath} already exists.  Please delete \
                it and try again"
            );
        }
    };
    info!("Writing to {outpath}");
    writeln!(outfile, "# This file was created by psync")?;
    writeln!(outfile, "# The length of the source file, in bytes")?;
    writeln!(outfile, "Length: {}", mmap.len())?;
    writeln!(outfile, "# The SHA-256 of the entire source file")?;
    writeln!(
        outfile,
        "SHA-256: {}",
        hex::encode(sha2::Sha256::digest(&mmap[..]))
    )?;
    writeln!(
        outfile,
        "# The number of bytes hashed to generate the start marks"
    )?;
    writeln!(outfile, "Window-Size: {}", crate::rollsum::WINDOW_SIZE)?;
    writeln!(
        outfile,
        "# The rest of this file defines chunks within the source file"
    )?;
    writeln!(outfile, "# from\tlength\tstart_mark\tsha-256")?;
    writeln!(outfile, "---")?;
    let mut pb = mk_bar(mmap.len())?;
    let mut breakpoints = vec![0, mmap.len()];
    if tar {
        breakpoints.extend(chunkers::chunk_tarball(&mmap[..]));
    }
    chunkers::split_large_chunks(&mut breakpoints, max_size);
    breakpoints.sort();
    breakpoints.dedup();
    for (from, to) in breakpoints.iter().zip(breakpoints.iter().skip(1)) {
        let ap = Appearance::new(&mmap[..], *from, to - from);
        writeln!(outfile, "{ap}")?;
        pb.update_to(ap.from);
    }
    pb.refresh();
    eprintln!();
    info!("Successfully created {outpath}");
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
    let file = File::open(seed)?;
    let mmap = unsafe { memmap2::Mmap::map(&file)? };

    if mmap.len() == control_file.total_len {
        let our_hash = Sha256::digest(&mmap[..]);
        if our_hash[..] == control_file.total_sha256[..] {
            info!("File is up-to-date");
            return Ok(());
        }
    }
    info!("Upstream file has changes");

    info!("Scanning local file for re-usable chunks");
    debug!(
        "Searching for {} chunks, appearing in {} positions",
        control_file.n_chunks(),
        control_file.n_appearances(),
    );
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
    let reusable_bytes = coverage
        .iter()
        .map(|(range, _)| range.end - range.start)
        .sum::<usize>();
    info!(
        "Able to re-use {} MiB of data from the local copy",
        reusable_bytes / 1024 / 1024,
    );
    let missing_chunks = coverage.gaps(&(0..control_file.total_len)).count();
    let missing_bytes = coverage
        .gaps(&(0..control_file.total_len))
        .map(|gap| gap.end - gap.start)
        .sum::<usize>();
    info!(
        "Need to download {} MiB of missing data in {} chunks",
        missing_bytes / 1024 / 1024,
        missing_chunks,
    );

    Ok(())
}
