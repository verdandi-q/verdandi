use std::ops::Bound;

use verdandi_core::ids::{Address, MessageId, SegmentId};
use verdandi_core::{Generation, Reason, Record, Timing, nth_id};

pub const MAX_STREAM_BYTES: usize = 406;

const ACK: u8 = 1;
const FAILURE: u8 = 2;
const GENERATION: u8 = 3;
const FORGET: u8 = 4;
const READ_SET: u8 = 5;
const TOMBSTONE: u8 = 6;
const REDRIVE: u8 = 7;
const KEY: u8 = 8;
const DAMAGE: u8 = 9;

const DELAY: u8 = 1;
const EXPIRE: u8 = 2;
const ORIGIN: u8 = 4;
const HEADERS: u8 = 8;

const ID_LEN: usize = 18;
const RUN_ENTRY_LEN: usize = 2 + 16;
const MESSAGE_FIXED_LEN: usize = 1 + 4 + ID_LEN + 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("a record in a block body is malformed")]
pub struct Malformed;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Origin {
    pub id: MessageId,
    pub stream: String,
    pub reason: Reason,
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    pub position: u32,
    pub id: MessageId,
    pub timing: Timing,
    pub origin: Option<Origin>,
    pub headers: Vec<(String, String)>,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Control {
    Record(Record),
    Key {
        scope: String,
        key: String,
        first: MessageId,
        count: u32,
    },
    Damage(Damage),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Damage {
    pub segment: SegmentId,
    pub start: u64,
    pub end: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunEntry {
    pub stream: String,
    pub offset: u32,
    pub len: u32,
    pub count: u32,
    pub crc: u32,
}

#[derive(Debug)]
pub struct Body<'a> {
    pub runs: Vec<(RunEntry, &'a [u8])>,
    pub controls: Vec<Control>,
}

pub fn run_entry_len(stream: &str) -> usize {
    RUN_ENTRY_LEN + stream.len()
}

pub fn message_len(stream: &str, message: &Message) -> usize {
    let origin = message.origin.as_ref().map_or(0, |o| {
        ID_LEN + 2 + o.stream.len() + 1 + 1 + o.error.as_ref().map_or(0, |e| 4 + e.len())
    });
    run_entry_len(stream)
        + overhead_len(message.timing, &message.headers)
        + origin
        + message.value.len()
}

pub fn overhead_len(timing: Timing, headers: &[(String, String)]) -> usize {
    let headers = match headers.len() {
        0 => 0,
        _ => {
            2 + headers
                .iter()
                .map(|(k, v)| 4 + k.len() + v.len())
                .sum::<usize>()
        }
    };
    let timing =
        8 * (usize::from(timing.delay_until.is_some()) + usize::from(timing.expire_at.is_some()));
    MESSAGE_FIXED_LEN + timing + headers
}

pub fn control_len(control: &Control) -> usize {
    let mut out = Vec::new();
    put_control(&mut out, control);
    out.len()
}

pub fn encode(runs: &[(&str, &[Message])], controls: &[Control]) -> (Vec<u8>, Vec<RunEntry>) {
    let records: Vec<(&str, u32, Vec<u8>)> = runs
        .iter()
        .map(|(stream, messages)| {
            let mut run = Vec::new();
            for message in *messages {
                put_message(&mut run, message);
            }
            (*stream, messages.len() as u32, run)
        })
        .collect();
    let raw: Vec<(&str, u32, &[u8])> = records
        .iter()
        .map(|(stream, count, run)| (*stream, *count, &run[..]))
        .collect();
    assemble(&raw, controls)
}

pub fn assemble(runs: &[(&str, u32, &[u8])], controls: &[Control]) -> (Vec<u8>, Vec<RunEntry>) {
    let table: usize = runs.iter().map(|(s, _, _)| RUN_ENTRY_LEN + s.len()).sum();
    let mut out = Vec::new();
    put_u32(&mut out, runs.len() as u32);
    let mut offset = 4 + table;
    let mut entries = Vec::with_capacity(runs.len());
    for (stream, count, run) in runs {
        let entry = RunEntry {
            stream: (*stream).to_owned(),
            offset: offset as u32,
            len: run.len() as u32,
            count: *count,
            crc: crc32c::crc32c(run),
        };
        put_str16(&mut out, stream);
        put_u32(&mut out, entry.offset);
        put_u32(&mut out, entry.len);
        put_u32(&mut out, entry.count);
        put_u32(&mut out, entry.crc);
        offset += run.len();
        entries.push(entry);
    }
    for (_, _, run) in runs {
        out.extend_from_slice(run);
    }
    for control in controls {
        put_control(&mut out, control);
    }
    (out, entries)
}

pub fn first_position(run: &[u8]) -> Result<u32, Malformed> {
    let mut r = Reader { b: run, at: 1 };
    r.u32()
}

pub fn decode(body: &[u8]) -> Result<Body<'_>, Malformed> {
    let mut r = Reader { b: body, at: 0 };
    let count = r.u32()?;
    let mut entries = Vec::new();
    for _ in 0..count {
        entries.push(RunEntry {
            stream: r.name()?,
            offset: r.u32()?,
            len: r.u32()?,
            count: r.u32()?,
            crc: r.u32()?,
        });
    }
    let mut runs = Vec::with_capacity(entries.len());
    for entry in entries {
        if entry.offset as usize != r.at {
            return Err(Malformed);
        }
        let bytes = r.take(entry.len as usize)?;
        runs.push((entry, bytes));
    }
    let mut controls = Vec::new();
    while r.at < body.len() {
        controls.push(r.control()?);
    }
    Ok(Body { runs, controls })
}

pub fn decode_run(run: &[u8], count: u32) -> Result<Vec<Message>, Malformed> {
    let mut r = Reader { b: run, at: 0 };
    let mut out: Vec<Message> = Vec::new();
    for n in 0..count {
        let m = r.message()?;
        if let Some(first) = out.first()
            && (first.position.checked_add(n) != Some(m.position)
                || nth_id(first.id, n) != Some(m.id))
        {
            return Err(Malformed);
        }
        out.push(m);
    }
    if count == 0 || r.at != run.len() {
        return Err(Malformed);
    }
    Ok(out)
}

fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_str16(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u16).to_le_bytes());
    out.extend_from_slice(s.as_bytes());
}

fn put_bytes32(out: &mut Vec<u8>, b: &[u8]) {
    put_u32(out, b.len() as u32);
    out.extend_from_slice(b);
}

fn put_error(out: &mut Vec<u8>, error: &Option<String>) {
    match error {
        Some(e) => {
            out.push(1);
            put_bytes32(out, e.as_bytes());
        }
        None => out.push(0),
    }
}

fn put_id(out: &mut Vec<u8>, id: MessageId) {
    put_u64(out, id.ms());
    put_u64(out, id.incarnation);
    out.extend_from_slice(&id.counter.to_le_bytes());
}

fn put_generation(out: &mut Vec<u8>, g: Generation) {
    put_u64(out, g.counter);
    put_u64(out, g.incarnation);
}

fn put_segment(out: &mut Vec<u8>, s: SegmentId) {
    put_u64(out, s.incarnation);
    put_u32(out, s.seq);
}

fn put_address(out: &mut Vec<u8>, a: Address) {
    put_segment(out, a.segment);
    put_u32(out, a.position);
}

fn reason_code(reason: Reason) -> u8 {
    match reason {
        Reason::Dead => 1,
        Reason::Expired => 2,
        Reason::Attempts => 3,
    }
}

fn put_message(out: &mut Vec<u8>, m: &Message) {
    let flags = [
        (m.timing.delay_until.is_some(), DELAY),
        (m.timing.expire_at.is_some(), EXPIRE),
        (m.origin.is_some(), ORIGIN),
        (!m.headers.is_empty(), HEADERS),
    ]
    .iter()
    .filter(|(set, _)| *set)
    .fold(0, |f, (_, bit)| f | bit);
    out.push(flags);
    put_u32(out, m.position);
    put_id(out, m.id);
    if let Some(t) = m.timing.delay_until {
        put_u64(out, t);
    }
    if let Some(t) = m.timing.expire_at {
        put_u64(out, t);
    }
    if let Some(o) = &m.origin {
        put_id(out, o.id);
        put_str16(out, &o.stream);
        out.push(reason_code(o.reason));
        put_error(out, &o.error);
    }
    if !m.headers.is_empty() {
        out.extend_from_slice(&(m.headers.len() as u16).to_le_bytes());
        for (k, v) in &m.headers {
            put_str16(out, k);
            put_str16(out, v);
        }
    }
    put_bytes32(out, &m.value);
}

fn put_control(out: &mut Vec<u8>, control: &Control) {
    match control {
        Control::Record(Record::Ack {
            group,
            generation,
            address,
            stream,
        }) => {
            out.push(ACK);
            put_str16(out, group);
            put_generation(out, *generation);
            put_address(out, *address);
            put_str16(out, stream);
        }
        Control::Record(Record::Failure {
            group,
            generation,
            address,
            stream,
            count,
            not_before,
            error,
        }) => {
            out.push(FAILURE);
            put_str16(out, group);
            put_generation(out, *generation);
            put_address(out, *address);
            put_str16(out, stream);
            put_u32(out, *count);
            put_u64(out, *not_before);
            put_error(out, error);
        }
        Control::Record(Record::Generation {
            group,
            generation,
            from,
            issued,
        }) => {
            out.push(GENERATION);
            put_str16(out, group);
            put_generation(out, *generation);
            match from {
                Bound::Unbounded => out.push(0),
                Bound::Included(id) => {
                    out.push(1);
                    put_id(out, *id);
                }
                Bound::Excluded(id) => {
                    out.push(if *issued { 3 } else { 2 });
                    put_id(out, *id);
                }
            }
        }
        Control::Record(Record::Forget {
            group,
            generation,
            provisional,
        }) => {
            out.push(FORGET);
            put_str16(out, group);
            put_generation(out, *generation);
            out.push(u8::from(*provisional));
        }
        Control::Record(Record::ReadSet {
            group,
            generation,
            selector,
        }) => {
            out.push(READ_SET);
            put_str16(out, group);
            put_generation(out, *generation);
            put_str16(out, selector);
        }
        Control::Record(Record::Tombstone { stream, up_to }) => {
            out.push(TOMBSTONE);
            put_str16(out, stream);
            put_id(out, *up_to);
        }
        Control::Record(Record::Redrive {
            group,
            generation,
            stream,
            up_to,
        }) => {
            out.push(REDRIVE);
            put_str16(out, group);
            put_generation(out, *generation);
            put_str16(out, stream);
            put_id(out, *up_to);
        }
        Control::Record(Record::Messages { .. }) => {
            unreachable!("message records are runs, not control records")
        }
        Control::Key {
            scope,
            key,
            first,
            count,
        } => {
            out.push(KEY);
            put_str16(out, scope);
            put_str16(out, key);
            put_id(out, *first);
            put_u32(out, *count);
        }
        Control::Damage(d) => {
            out.push(DAMAGE);
            put_segment(out, d.segment);
            put_u64(out, d.start);
            put_u64(out, d.end);
        }
    }
}

struct Reader<'a> {
    b: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], Malformed> {
        let end = self.at.checked_add(n).ok_or(Malformed)?;
        let out = self.b.get(self.at..end).ok_or(Malformed)?;
        self.at = end;
        Ok(out)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], Malformed> {
        let mut a = [0; N];
        a.copy_from_slice(self.take(N)?);
        Ok(a)
    }

    fn u8(&mut self) -> Result<u8, Malformed> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, Malformed> {
        self.array().map(u16::from_le_bytes)
    }

    fn u32(&mut self) -> Result<u32, Malformed> {
        self.array().map(u32::from_le_bytes)
    }

    fn u64(&mut self) -> Result<u64, Malformed> {
        self.array().map(u64::from_le_bytes)
    }

    fn string(&mut self, n: usize) -> Result<String, Malformed> {
        String::from_utf8(self.take(n)?.to_vec()).map_err(|_| Malformed)
    }

    fn str16(&mut self) -> Result<String, Malformed> {
        let n = self.u16()? as usize;
        self.string(n)
    }

    fn name(&mut self) -> Result<String, Malformed> {
        let name = self.str16()?;
        if name.len() > MAX_STREAM_BYTES {
            return Err(Malformed);
        }
        Ok(name)
    }

    fn error(&mut self) -> Result<Option<String>, Malformed> {
        match self.u8()? {
            0 => Ok(None),
            1 => {
                let n = self.u32()? as usize;
                self.string(n).map(Some)
            }
            _ => Err(Malformed),
        }
    }

    fn flag(&mut self) -> Result<bool, Malformed> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Malformed),
        }
    }

    fn id(&mut self) -> Result<MessageId, Malformed> {
        let (ms, incarnation, counter) = (self.u64()?, self.u64()?, self.u16()?);
        MessageId::new(ms, incarnation, counter).ok_or(Malformed)
    }

    fn generation(&mut self) -> Result<Generation, Malformed> {
        Ok(Generation {
            counter: self.u64()?,
            incarnation: self.u64()?,
        })
    }

    fn segment(&mut self) -> Result<SegmentId, Malformed> {
        Ok(SegmentId {
            incarnation: self.u64()?,
            seq: self.u32()?,
        })
    }

    fn address(&mut self) -> Result<Address, Malformed> {
        Ok(Address {
            segment: self.segment()?,
            position: self.u32()?,
        })
    }

    fn time(&mut self, flags: u8, bit: u8) -> Result<Option<u64>, Malformed> {
        match flags & bit {
            0 => Ok(None),
            _ => self.u64().map(Some),
        }
    }

    fn message(&mut self) -> Result<Message, Malformed> {
        let flags = self.u8()?;
        if flags & !(DELAY | EXPIRE | ORIGIN | HEADERS) != 0 {
            return Err(Malformed);
        }
        let position = self.u32()?;
        let id = self.id()?;
        let timing = Timing {
            delay_until: self.time(flags, DELAY)?,
            expire_at: self.time(flags, EXPIRE)?,
        };
        let origin = if flags & ORIGIN != 0 {
            Some(Origin {
                id: self.id()?,
                stream: self.name()?,
                reason: match self.u8()? {
                    1 => Reason::Dead,
                    2 => Reason::Expired,
                    3 => Reason::Attempts,
                    _ => return Err(Malformed),
                },
                error: self.error()?,
            })
        } else {
            None
        };
        let mut headers = Vec::new();
        if flags & HEADERS != 0 {
            for _ in 0..self.u16()? {
                headers.push((self.str16()?, self.str16()?));
            }
        }
        let n = self.u32()? as usize;
        Ok(Message {
            position,
            id,
            timing,
            origin,
            headers,
            value: self.take(n)?.to_vec(),
        })
    }

    fn control(&mut self) -> Result<Control, Malformed> {
        let record = match self.u8()? {
            ACK => Record::Ack {
                group: self.name()?,
                generation: self.generation()?,
                address: self.address()?,
                stream: self.name()?,
            },
            FAILURE => Record::Failure {
                group: self.name()?,
                generation: self.generation()?,
                address: self.address()?,
                stream: self.name()?,
                count: self.u32()?,
                not_before: self.u64()?,
                error: self.error()?,
            },
            GENERATION => {
                let group = self.name()?;
                let generation = self.generation()?;
                let tag = self.u8()?;
                let from = match tag {
                    0 => Bound::Unbounded,
                    1 => Bound::Included(self.id()?),
                    2 | 3 => Bound::Excluded(self.id()?),
                    _ => return Err(Malformed),
                };
                Record::Generation {
                    group,
                    generation,
                    from,
                    issued: tag == 3,
                }
            }
            FORGET => Record::Forget {
                group: self.name()?,
                generation: self.generation()?,
                provisional: self.flag()?,
            },
            READ_SET => Record::ReadSet {
                group: self.name()?,
                generation: self.generation()?,
                selector: self.name()?,
            },
            TOMBSTONE => Record::Tombstone {
                stream: self.name()?,
                up_to: self.id()?,
            },
            REDRIVE => Record::Redrive {
                group: self.name()?,
                generation: self.generation()?,
                stream: self.name()?,
                up_to: self.id()?,
            },
            KEY => {
                return Ok(Control::Key {
                    scope: self.name()?,
                    key: self.str16()?,
                    first: self.id()?,
                    count: self.u32()?,
                });
            }
            DAMAGE => {
                return Ok(Control::Damage(Damage {
                    segment: self.segment()?,
                    start: self.u64()?,
                    end: self.u64()?,
                }));
            }
            _ => return Err(Malformed),
        };
        Ok(Control::Record(record))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(ms: u64, counter: u16) -> MessageId {
        MessageId::new(ms, 9, counter).unwrap()
    }

    fn generation() -> Generation {
        Generation {
            counter: 3,
            incarnation: 4,
        }
    }

    fn address() -> Address {
        Address {
            segment: SegmentId {
                incarnation: 5,
                seq: 6,
            },
            position: 7,
        }
    }

    fn message(position: u32, origin: Option<Origin>) -> Message {
        Message {
            position,
            id: id(100, position as u16),
            timing: Timing {
                delay_until: Some(5),
                expire_at: None,
            },
            origin,
            headers: vec![("content-type".to_owned(), "text/plain".to_owned())],
            value: b"hello".to_vec(),
        }
    }

    fn every_control() -> Vec<Control> {
        let g = "billing".to_owned();
        let s = "orders".to_owned();
        let mut controls: Vec<Control> = [
            Record::Ack {
                group: g.clone(),
                generation: generation(),
                address: address(),
                stream: s.clone(),
            },
            Record::Failure {
                group: g.clone(),
                generation: generation(),
                address: address(),
                stream: s.clone(),
                count: 4,
                not_before: 99,
                error: Some("boom".to_owned()),
            },
            Record::Generation {
                group: g.clone(),
                generation: generation(),
                from: Bound::Excluded(id(1, 2)),
                issued: false,
            },
            Record::Generation {
                group: g.clone(),
                generation: generation(),
                from: Bound::Excluded(id(1, 2)),
                issued: true,
            },
            Record::Generation {
                group: g.clone(),
                generation: generation(),
                from: Bound::Included(id(1, 2)),
                issued: false,
            },
            Record::Generation {
                group: g.clone(),
                generation: generation(),
                from: Bound::Unbounded,
                issued: false,
            },
            Record::Forget {
                group: g.clone(),
                generation: generation(),
                provisional: true,
            },
            Record::ReadSet {
                group: g.clone(),
                generation: generation(),
                selector: "orders.*".to_owned(),
            },
            Record::Tombstone {
                stream: s.clone(),
                up_to: id(8, 0),
            },
            Record::Redrive {
                group: g,
                generation: generation(),
                stream: "orders.dead.billing".to_owned(),
                up_to: id(9, 1),
            },
        ]
        .into_iter()
        .map(Control::Record)
        .collect();
        controls.push(Control::Key {
            scope: "/bulk".to_owned(),
            key: "order-1".to_owned(),
            first: id(10, 0),
            count: 1_000_000,
        });
        controls.push(Control::Damage(Damage {
            segment: address().segment,
            start: 4096,
            end: 8192,
        }));
        controls
    }

    #[test]
    fn every_record_kind_of_format_1_round_trips() {
        let origin = Origin {
            id: id(1, 1),
            stream: "orders".to_owned(),
            reason: Reason::Expired,
            error: None,
        };
        let first = [message(0, None), message(1, None)];
        let dead = [message(2, Some(origin))];
        let controls = every_control();
        let (body, entries) = encode(
            &[("orders", &first), ("orders.dead.billing", &dead)],
            &controls,
        );
        let decoded = decode(&body).unwrap();
        assert_eq!(decoded.controls, controls);
        let runs: Vec<_> = decoded
            .runs
            .iter()
            .map(|(entry, bytes)| {
                assert_eq!(entry.crc, crc32c::crc32c(bytes));
                (entry.clone(), decode_run(bytes, entry.count).unwrap())
            })
            .collect();
        assert_eq!(runs[0].1, first);
        assert_eq!(runs[1].1, dead);
        assert_eq!(
            runs.iter().map(|(e, _)| e.clone()).collect::<Vec<_>>(),
            entries
        );
    }

    #[test]
    fn the_computed_length_is_the_encoded_length() {
        let m = message(0, None);
        let mut bare = message(1, None);
        bare.timing = Timing::default();
        bare.headers.clear();
        for m in [m, bare] {
            let (body, _) = encode(&[("orders", std::slice::from_ref(&m))], &[]);
            assert_eq!(4 + message_len("orders", &m), body.len());
        }
        let controls = every_control();
        let (body, _) = encode(&[], &controls);
        assert_eq!(
            4 + controls.iter().map(control_len).sum::<usize>(),
            body.len()
        );
    }

    #[test]
    fn the_stream_field_holds_a_406_byte_dead_stream_name_and_no_more() {
        let name = format!("{}.dead.{}", "s".repeat(200), "g".repeat(200));
        assert_eq!(name.len(), MAX_STREAM_BYTES);
        let m = [message(0, None)];
        let (body, _) = encode(&[(&name, &m)], &[]);
        assert_eq!(decode(&body).unwrap().runs[0].0.stream, name);
        let longer = format!("{name}x");
        let (body, _) = encode(&[(&longer, &m)], &[]);
        assert_eq!(decode(&body).err(), Some(Malformed));
    }

    #[test]
    fn a_batch_of_a_million_one_byte_messages_fits_one_block() {
        let small = Message {
            position: 0,
            id: id(1, 0),
            timing: Timing::default(),
            origin: None,
            headers: Vec::new(),
            value: vec![b'1'],
        };
        let record = message_len("s", &small) - RUN_ENTRY_LEN - 1;
        assert_eq!(record, 28);
        let block = 4 + RUN_ENTRY_LEN + 1 + 1_000_000 * record + control_len(&every_control()[9]);
        assert!(block <= crate::block::MAX_BLOCK - crate::block::BLOCK_HEADER_LEN);
    }

    #[test]
    fn a_run_whose_positions_or_ids_are_not_consecutive_is_malformed() {
        let (good, _) = encode(&[("s", &[message(0, None), message(1, None)])], &[]);
        assert!(decode(&good).is_ok_and(|b| decode_run(b.runs[0].1, 2).is_ok()));
        let mut gap = message(2, None);
        gap.position = 5;
        let (bad, _) = encode(&[("s", &[message(0, None), gap])], &[]);
        assert_eq!(
            decode_run(decode(&bad).unwrap().runs[0].1, 2),
            Err(Malformed)
        );
        let mut jump = message(1, None);
        jump.id = id(500, 0);
        let (bad, _) = encode(&[("s", &[message(0, None), jump])], &[]);
        assert_eq!(
            decode_run(decode(&bad).unwrap().runs[0].1, 2),
            Err(Malformed)
        );
    }

    #[test]
    fn a_truncated_or_unknown_record_is_malformed() {
        let (body, _) = encode(&[], &every_control());
        assert_eq!(decode(&body[..body.len() - 1]).err(), Some(Malformed));
        let mut unknown = body.clone();
        unknown.push(0xee);
        assert_eq!(decode(&unknown).err(), Some(Malformed));
    }
}
