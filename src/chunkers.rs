use tracing::*;

/// This is a byte-offset into a file.  The break occurs _before_ the
/// referenced byte.
pub type Breakpoint = usize;

pub fn chunk_tarball(file: &[u8]) -> impl Iterator<Item = Breakpoint> + '_ {
    let mut offset = 0;
    info!("file len: {}", file.len());
    std::iter::from_fn(move || {
        if offset + 512 >= file.len() {
            return None;
        }
        let hdr = tar::Header::from_byte_slice(&file[offset..offset + 512]);
        let data_len = hdr.entry_size().ok()? as usize;
        let x = ((data_len - 1) / 512) + 1; // round to 512 bytes
        let entry_len = (x + 1) * 512; // add 512 for the header
        let breakpoint = offset;
        offset += entry_len;
        Some(breakpoint)
    })
}

pub fn split_large_chunks(breakpoints: &mut Vec<Breakpoint>, max_size: usize) {
    breakpoints.sort();
    let new = breakpoints
        .iter()
        .zip(breakpoints.iter().skip(1))
        .flat_map(|(&from, &to)| {
            let len = to - from;
            let n = if len == 0 {
                0
            } else {
                (len - 1) / max_size + 1
            };
            if n > 1 {
                debug!(
                    "Large chunk found: {} KiB.  Splitting into {} pieces",
                    len / 1024,
                    n
                );
            }
            (0..n).map(move |i| from + i * max_size)
        })
        .chain(breakpoints.last().copied())
        .collect();
    *breakpoints = new;
}
