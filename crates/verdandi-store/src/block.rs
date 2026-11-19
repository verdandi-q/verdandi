use verdandi_core::ids::SegmentId;

pub const VERSION: u16 = 1;
pub const PAGE: u64 = 4096;
pub const SEGMENT_BYTES: u64 = 64 << 20;
pub const MAX_BLOCK: usize = 32 << 20;
pub const FILE_HEADER_LEN: usize = 19;
pub const BLOCK_HEADER_LEN: usize = 54;

const FILE_MAGIC: [u8; 4] = *b"VDLF";
const BLOCK_MAGIC: [u8; 4] = *b"VDLB";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileKind {
    Segment = 1,
    Compacted = 2,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileHeader {
    pub kind: FileKind,
    pub epoch: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockHeader {
    pub segment: SegmentId,
    pub offset: u64,
    pub epoch: u64,
    pub compaction_version: u32,
    pub commit_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Bad {
    Damaged,
    UnknownVersion(u16),
}

pub fn align(offset: u64) -> u64 {
    offset.div_ceil(PAGE) * PAGE
}

fn u16_at(b: &[u8], i: usize) -> u16 {
    let mut a = [0; 2];
    a.copy_from_slice(&b[i..i + 2]);
    u16::from_le_bytes(a)
}

fn u32_at(b: &[u8], i: usize) -> u32 {
    let mut a = [0; 4];
    a.copy_from_slice(&b[i..i + 4]);
    u32::from_le_bytes(a)
}

fn u64_at(b: &[u8], i: usize) -> u64 {
    let mut a = [0; 8];
    a.copy_from_slice(&b[i..i + 8]);
    u64::from_le_bytes(a)
}

impl FileHeader {
    pub fn encode(&self) -> [u8; FILE_HEADER_LEN] {
        let mut b = [0; FILE_HEADER_LEN];
        b[0..4].copy_from_slice(&FILE_MAGIC);
        b[4..6].copy_from_slice(&VERSION.to_le_bytes());
        b[6] = self.kind as u8;
        b[7..15].copy_from_slice(&self.epoch.to_le_bytes());
        let crc = crc32c::crc32c(&b[..15]);
        b[15..19].copy_from_slice(&crc.to_le_bytes());
        b
    }

    pub fn decode(data: &[u8]) -> Result<Self, Bad> {
        let b = data.get(..FILE_HEADER_LEN).ok_or(Bad::Damaged)?;
        if b[0..4] != FILE_MAGIC || crc32c::crc32c(&b[..15]) != u32_at(b, 15) {
            return Err(Bad::Damaged);
        }
        let version = u16_at(b, 4);
        if version != VERSION {
            return Err(Bad::UnknownVersion(version));
        }
        let kind = match b[6] {
            1 => FileKind::Segment,
            2 => FileKind::Compacted,
            _ => return Err(Bad::Damaged),
        };
        Ok(Self {
            kind,
            epoch: u64_at(b, 7),
        })
    }
}

pub fn encode_block(header: &BlockHeader, body: &[u8]) -> Vec<u8> {
    let mut b = Vec::with_capacity(BLOCK_HEADER_LEN + body.len());
    b.extend_from_slice(&BLOCK_MAGIC);
    b.extend_from_slice(&VERSION.to_le_bytes());
    b.extend_from_slice(&header.segment.incarnation.to_le_bytes());
    b.extend_from_slice(&header.segment.seq.to_le_bytes());
    b.extend_from_slice(&header.offset.to_le_bytes());
    b.extend_from_slice(&header.epoch.to_le_bytes());
    b.extend_from_slice(&header.compaction_version.to_le_bytes());
    b.extend_from_slice(&header.commit_ms.to_le_bytes());
    b.extend_from_slice(&(body.len() as u32).to_le_bytes());
    let crc = crc32c::crc32c_append(crc32c::crc32c(&b), body);
    b.extend_from_slice(&crc.to_le_bytes());
    b.extend_from_slice(body);
    b
}

pub fn decode_block(data: &[u8], offset: u64) -> Result<(BlockHeader, &[u8]), Bad> {
    let (header, version, body) = parse_block(data, offset)?;
    if version != VERSION {
        return Err(Bad::UnknownVersion(version));
    }
    Ok((header, body))
}

pub fn decode_bytes(block: &[u8], offset: u64) -> Result<(BlockHeader, &[u8]), Bad> {
    let (header, version, body) = parse_at(block, 0, offset)?;
    if version != VERSION {
        return Err(Bad::UnknownVersion(version));
    }
    if block.len() != BLOCK_HEADER_LEN + body.len() {
        return Err(Bad::Damaged);
    }
    Ok((header, body))
}

pub(crate) fn parse_block(data: &[u8], offset: u64) -> Result<(BlockHeader, u16, &[u8]), Bad> {
    let start = usize::try_from(offset).map_err(|_| Bad::Damaged)?;
    parse_at(data, start, offset)
}

fn parse_at(data: &[u8], start: usize, offset: u64) -> Result<(BlockHeader, u16, &[u8]), Bad> {
    let h = data
        .get(start..start + BLOCK_HEADER_LEN)
        .ok_or(Bad::Damaged)?;
    let len = u32_at(h, 46) as usize;
    if h[0..4] != BLOCK_MAGIC || len > MAX_BLOCK - BLOCK_HEADER_LEN {
        return Err(Bad::Damaged);
    }
    if u64_at(h, 18) != offset {
        return Err(Bad::Damaged);
    }
    let body_start = start + BLOCK_HEADER_LEN;
    let body = data.get(body_start..body_start + len).ok_or(Bad::Damaged)?;
    if crc32c::crc32c_append(crc32c::crc32c(&h[..50]), body) != u32_at(h, 50) {
        return Err(Bad::Damaged);
    }
    let header = BlockHeader {
        segment: SegmentId {
            incarnation: u64_at(h, 6),
            seq: u32_at(h, 14),
        },
        offset,
        epoch: u64_at(h, 26),
        compaction_version: u32_at(h, 34),
        commit_ms: u64_at(h, 38),
    };
    Ok((header, u16_at(h, 4), body))
}
