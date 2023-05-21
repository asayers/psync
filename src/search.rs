use crate::{rollsum::*, ControlFile, Sha256Sum};
use sha2::Digest;
use std::collections::HashMap;
use xorf::Filter;

pub fn search(
    file: &[u8],
    control_file: &ControlFile,
    mut progress_cb: impl FnMut(usize),
) -> anyhow::Result<HashMap<Sha256Sum, usize>> {
    let filter = control_file.mk_filter()?;
    let mut hasher = RollSum::default();
    let mut our_appearances = HashMap::<Sha256Sum, usize>::default();
    for (n_bytes, &x) in file.iter().enumerate() {
        hasher.input(x);
        let hash = hasher.sum();
        if n_bytes % 1_000 == 0 {
            progress_cb(n_bytes);
        }
        if filter.contains(&hash) {
            for (len, their_sha) in control_file.chunks.get(&hash).into_iter().flatten() {
                let our_start = n_bytes + 1 - WINDOW_SIZE;
                let end = file.len().min(our_start + len);
                let our_sha = sha2::Sha256::digest(&file[our_start..end]);
                if our_sha[..] == their_sha[..] {
                    our_appearances.insert(*their_sha, our_start);
                }
            }
        }
    }
    Ok(our_appearances)
}
