use mio::Token;

/// A contiguous block of [`Token`]s that a component may use when it
/// registers sources on a shared [`mio::Poll`].
///
/// A caller that hosts several components in one loop keeps a free range and
/// lets each of them carve its share off the front with [`TokenRange::take`],
/// so their tokens never overlap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenRange {
    start: usize,
    len: usize,
}

impl TokenRange {
    /// Tokens `start .. start + len`. The length is clamped so that the end
    /// does not overflow, and `Token(usize::MAX)` (reserved by mio) is never
    /// included.
    pub fn new(start: usize, len: usize) -> Self {
        let room = usize::MAX.saturating_sub(start);
        TokenRange {
            start,
            len: len.min(room),
        }
    }

    pub fn start(&self) -> usize {
        self.start
    }

    /// First value after the range.
    pub fn end(&self) -> usize {
        self.start + self.len
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn contains(&self, token: Token) -> bool {
        self.index_of(token).is_some()
    }

    /// The `index`-th token of the range.
    pub fn get(&self, index: usize) -> Option<Token> {
        (index < self.len).then(|| Token(self.start + index))
    }

    /// Position of `token` in the range, if it belongs to it.
    pub fn index_of(&self, token: Token) -> Option<usize> {
        token
            .0
            .checked_sub(self.start)
            .filter(|&index| index < self.len)
    }

    /// Split the first `n` tokens off the front, leaving the rest in `self`.
    /// Returns `None` (and changes nothing) if fewer than `n` are left.
    pub fn take(&mut self, n: usize) -> Option<TokenRange> {
        if n > self.len {
            return None;
        }
        let taken = TokenRange {
            start: self.start,
            len: n,
        };
        self.start += n;
        self.len -= n;
        Some(taken)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_and_index_of_are_inverse() {
        let r = TokenRange::new(10, 3);
        assert_eq!(r.get(0), Some(Token(10)));
        assert_eq!(r.get(2), Some(Token(12)));
        assert_eq!(r.get(3), None);
        assert_eq!(r.index_of(Token(11)), Some(1));
        assert_eq!(r.index_of(Token(9)), None);
        assert_eq!(r.index_of(Token(13)), None);
        assert!(r.contains(Token(12)) && !r.contains(Token(13)));
    }

    #[test]
    fn take_carves_disjoint_ranges() {
        let mut free = TokenRange::new(0, 10);
        let a = free.take(4).unwrap();
        let b = free.take(5).unwrap();
        assert_eq!((a.start(), a.end()), (0, 4));
        assert_eq!((b.start(), b.end()), (4, 9));
        assert_eq!(free.len(), 1);
        assert_eq!(free.take(2), None);
        assert_eq!(free.len(), 1);
    }

    #[test]
    fn length_is_clamped() {
        let r = TokenRange::new(usize::MAX - 1, 10);
        assert_eq!(r.len(), 1);
        assert!(!r.contains(Token(usize::MAX)));
    }
}
