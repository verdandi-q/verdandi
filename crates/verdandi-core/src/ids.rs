use std::fmt;
use std::str::FromStr;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MessageId {
    ms: u64,
    pub incarnation: u64,
    pub counter: u16,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SegmentId {
    pub incarnation: u64,
    pub seq: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Address {
    pub segment: SegmentId,
    pub position: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParseIdError;

impl fmt::Display for ParseIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid id")
    }
}

impl std::error::Error for ParseIdError {}

fn hex_fields<const N: usize>(s: &str, widths: [usize; N]) -> Result<[u64; N], ParseIdError> {
    let mut parts = s.split('-');
    let mut out = [0; N];
    for (slot, width) in out.iter_mut().zip(widths) {
        let part = parts.next().ok_or(ParseIdError)?;
        if part.len() != width || !part.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
            return Err(ParseIdError);
        }
        *slot = u64::from_str_radix(part, 16).map_err(|_| ParseIdError)?;
    }
    match parts.next() {
        None => Ok(out),
        Some(_) => Err(ParseIdError),
    }
}

impl MessageId {
    pub const MAX_MS: u64 = 0xffff_ffff_ffff;

    pub fn new(ms: u64, incarnation: u64, counter: u16) -> Option<Self> {
        (ms <= Self::MAX_MS).then_some(Self {
            ms,
            incarnation,
            counter,
        })
    }

    pub fn ms(&self) -> u64 {
        self.ms
    }
}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:012x}-{:016x}-{:04x}",
            self.ms, self.incarnation, self.counter
        )
    }
}

impl FromStr for MessageId {
    type Err = ParseIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let [ms, incarnation, counter] = hex_fields(s, [12, 16, 4])?;
        Ok(Self {
            ms,
            incarnation,
            counter: counter as u16,
        })
    }
}

impl fmt::Display for SegmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}-{:08x}", self.incarnation, self.seq)
    }
}

impl FromStr for SegmentId {
    type Err = ParseIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let [incarnation, seq] = hex_fields(s, [16, 8])?;
        Ok(Self {
            incarnation,
            seq: seq as u32,
        })
    }
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{:08x}", self.segment, self.position)
    }
}

impl FromStr for Address {
    type Err = ParseIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let [incarnation, seq, position] = hex_fields(s, [16, 8, 8])?;
        Ok(Self {
            segment: SegmentId {
                incarnation,
                seq: seq as u32,
            },
            position: position as u32,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID: MessageId = MessageId {
        ms: 0x0192_3a4b_5c6d,
        incarnation: 0x89ab_cdef_0123_4567,
        counter: 0x002a,
    };

    #[test]
    fn message_id_text_is_fixed_width_lowercase_hex() {
        assert_eq!(ID.to_string(), "01923a4b5c6d-89abcdef01234567-002a");
        let small = MessageId {
            ms: 1,
            incarnation: 2,
            counter: 3,
        };
        assert_eq!(small.to_string(), "000000000001-0000000000000002-0003");
    }

    #[test]
    fn message_id_new_rejects_ms_wider_than_48_bits() {
        let max = MessageId::new(MessageId::MAX_MS, 1, 2).unwrap();
        assert_eq!(max.ms(), MessageId::MAX_MS);
        assert_eq!(max.to_string().parse::<MessageId>(), Ok(max));
        assert_eq!(MessageId::new(MessageId::MAX_MS + 1, 1, 2), None);
        assert_eq!(MessageId::new(u64::MAX, 1, 2), None);
    }

    #[test]
    fn message_id_round_trips_through_text() {
        for id in [
            ID,
            MessageId {
                ms: 0,
                incarnation: 0,
                counter: 0,
            },
            MessageId {
                ms: 0xffff_ffff_ffff,
                incarnation: u64::MAX,
                counter: u16::MAX,
            },
        ] {
            assert_eq!(id.to_string().parse::<MessageId>(), Ok(id));
        }
    }

    #[test]
    fn message_id_text_order_matches_value_order() {
        let mut ids = [
            MessageId {
                counter: 0x100,
                ..ID
            },
            MessageId {
                ms: ID.ms + 1,
                counter: 0,
                ..ID
            },
            ID,
            MessageId {
                incarnation: ID.incarnation + 1,
                ..ID
            },
        ];
        let mut texts: Vec<String> = ids.iter().map(ToString::to_string).collect();
        ids.sort();
        texts.sort();
        assert_eq!(
            texts,
            ids.iter().map(ToString::to_string).collect::<Vec<_>>()
        );
    }

    #[test]
    fn message_id_parse_rejects_bad_text() {
        for bad in [
            "",
            "01923a4b5c6d-89abcdef01234567",
            "01923a4b5c6d-89abcdef01234567-002a-0",
            "01923A4B5C6D-89abcdef01234567-002a",
            "1923a4b5c6d-89abcdef01234567-002a",
            "001923a4b5c6d-89abcdef01234567-002a",
            "01923a4b5c6d-89abcdef01234567-+02a",
            "01923a4b5c6d-89abcdef01234567-002g",
            "01923a4b5c6d_89abcdef01234567_002a",
        ] {
            assert_eq!(bad.parse::<MessageId>(), Err(ParseIdError), "{bad}");
        }
    }

    #[test]
    fn segment_id_round_trips_through_text() {
        let id = SegmentId {
            incarnation: 0x89ab_cdef_0123_4567,
            seq: 7,
        };
        assert_eq!(id.to_string(), "89abcdef01234567-00000007");
        assert_eq!(id.to_string().parse::<SegmentId>(), Ok(id));
    }

    #[test]
    fn segment_id_parse_rejects_bad_text() {
        for bad in [
            "",
            "89abcdef01234567",
            "89abcdef01234567-0000007",
            "89abcdef01234567-00000007-00000000",
        ] {
            assert_eq!(bad.parse::<SegmentId>(), Err(ParseIdError), "{bad}");
        }
    }

    #[test]
    fn address_round_trips_through_text() {
        let address = Address {
            segment: SegmentId {
                incarnation: u64::MAX,
                seq: u32::MAX,
            },
            position: 0x10,
        };
        assert_eq!(address.to_string(), "ffffffffffffffff-ffffffff-00000010");
        assert_eq!(address.to_string().parse::<Address>(), Ok(address));
    }

    #[test]
    fn address_parse_rejects_bad_text() {
        for bad in [
            "",
            "ffffffffffffffff-ffffffff",
            "ffffffffffffffff-ffffffff-0000010",
            "ffffffffffffffff-ffffffff-00000010-00",
        ] {
            assert_eq!(bad.parse::<Address>(), Err(ParseIdError), "{bad}");
        }
    }
}
