// Stolen from oll3/bita
pub struct RollSum {
    s1: u64,
    s2: u64,
    offset: usize,
    window: [u8; WINDOW_SIZE],
}

const CHAR_OFFSET: u64 = 63;
pub const WINDOW_SIZE: usize = 4 * 1024;

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
