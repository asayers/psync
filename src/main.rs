use anyhow::anyhow;
use clap::Parser;
use kdam::{Bar, BarExt};
use psync::*;
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
        #[clap(long, short, default_value = "65536")]
        size: usize,
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
        Cmd::Chunk { path, size } => chunk(path, size),
    }
}

fn chunk(path: PathBuf, size: usize) -> anyhow::Result<()> {
    println!("# from\tlength\tstart_mark\tsha-256");
    let file = File::open(path)?;
    let mmap = unsafe { memmap2::Mmap::map(&file)? };
    let mut pb = mk_bar(mmap.len())?;
    for (from, size, start_mark, hash) in chunkers::chunk_uniform(&mmap[..], size)? {
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
    Ok(())
}
