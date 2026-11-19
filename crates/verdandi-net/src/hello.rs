use std::io::{Error, ErrorKind, Result};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::link::{ADDR_MAX, HELLO_LABEL, handshake};

pub const HELLO_MAX: usize = 1024;
pub const MEMBERS_MAX: usize = 64;
const REPLY_MAX: usize = 16 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hello {
    pub node: u64,
    pub addr: String,
    pub incarnation: u64,
    pub sent_ms: u64,
    pub cluster: Option<ClusterId>,
}

pub type ClusterId = (u64, u64);

pub type Members = Vec<(u64, String, bool)>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reply {
    pub cluster: Option<ClusterId>,
    pub members: Members,
}

fn bad<E: Into<Box<dyn std::error::Error + Send + Sync>>>(e: E) -> Error {
    Error::new(ErrorKind::InvalidData, e)
}

fn seal_of<T: Serialize>(key: &[u8], value: &T) -> Result<Vec<u8>> {
    let payload = postcard::to_allocvec(value).map_err(bad)?;
    let mut hs = handshake(HELLO_LABEL, key, true)?;
    let mut out = vec![0u8; payload.len() + 80];
    let n = hs.write_message(&payload, &mut out).map_err(bad)?;
    out.truncate(n);
    Ok(out)
}

fn open_of<T: DeserializeOwned>(key: &[u8], bytes: &[u8], max: usize) -> Result<T> {
    if bytes.len() > max {
        return Err(bad("hello too large"));
    }
    let mut hs = handshake(HELLO_LABEL, key, false)?;
    let mut out = vec![0u8; max];
    let n = hs.read_message(bytes, &mut out).map_err(bad)?;
    postcard::from_bytes(&out[..n]).map_err(bad)
}

pub fn seal(key: &[u8], hello: &Hello) -> Result<Vec<u8>> {
    if hello.addr.len() > ADDR_MAX {
        return Err(bad("address too long"));
    }
    seal_of(key, hello)
}

pub fn open(key: &[u8], bytes: &[u8]) -> Result<Hello> {
    let hello: Hello = open_of(key, bytes, HELLO_MAX)?;
    if hello.addr.len() > ADDR_MAX {
        return Err(bad("address too long"));
    }
    Ok(hello)
}

pub fn seal_members(key: &[u8], reply: &Reply) -> Result<Vec<u8>> {
    let members = &reply.members;
    if members.len() > MEMBERS_MAX || members.iter().any(|m| m.1.len() > ADDR_MAX) {
        return Err(bad("member list too large"));
    }
    seal_of(key, reply)
}

pub fn open_members(key: &[u8], bytes: &[u8]) -> Result<Reply> {
    let reply: Reply = open_of(key, bytes, REPLY_MAX)?;
    let members = &reply.members;
    if members.len() > MEMBERS_MAX || members.iter().any(|m| m.1.len() > ADDR_MAX) {
        return Err(bad("member list too large"));
    }
    Ok(reply)
}
