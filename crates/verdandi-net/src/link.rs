use std::future::Future;
use std::io::{Error, ErrorKind, Result};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use sha2::{Digest, Sha256};
use snow::{Builder, HandshakeState, StatelessTransportState};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::time::timeout;
use verdandi_node::Msg;

use crate::{decode, encode};

pub const MAX_FRAME: usize = 16 << 20;
const NOISE_MAX: usize = 65535;
const CHUNK: usize = NOISE_MAX - 16 - 1;
const HELLO_MAX: usize = 8 + ADDR_MAX;
pub(crate) const ADDR_MAX: usize = 255;
const KEY_MIN: usize = 32;
const IDLE: Duration = Duration::from_secs(5);
const WRITE: Duration = Duration::from_secs(5);
const PARAMS: &str = "Noise_NNpsk0_25519_ChaChaPoly_SHA256";
const LINK_LABEL: &[u8] = b"verdandi link v1";
pub(crate) const HELLO_LABEL: &[u8] = b"verdandi hello v1";

fn bad<E: Into<Box<dyn std::error::Error + Send + Sync>>>(e: E) -> Error {
    Error::new(ErrorKind::InvalidData, e)
}

pub fn key_ok(key: &[u8]) -> Result<()> {
    if key.len() < KEY_MIN {
        return Err(bad("cluster key under 32 bytes"));
    }
    Ok(())
}

pub(crate) fn handshake(label: &[u8], key: &[u8], initiator: bool) -> Result<HandshakeState> {
    let psk: [u8; 32] = Sha256::new()
        .chain_update(label)
        .chain_update(key)
        .finalize()
        .into();
    let builder = Builder::new(PARAMS.parse().map_err(bad)?)
        .prologue(label)
        .map_err(bad)?
        .psk(0, &psk)
        .map_err(bad)?;
    if initiator {
        builder.build_initiator()
    } else {
        builder.build_responder()
    }
    .map_err(bad)
}

async fn limit<T>(d: Duration, f: impl Future<Output = Result<T>>) -> Result<T> {
    timeout(d, f)
        .await
        .unwrap_or_else(|_| Err(ErrorKind::TimedOut.into()))
}

async fn read_raw(stream: &mut OwnedReadHalf, buf: &mut [u8]) -> Result<usize> {
    let len = stream.read_u32().await? as usize;
    if len > NOISE_MAX {
        return Err(bad("noise message too large"));
    }
    stream.read_exact(&mut buf[..len]).await?;
    Ok(len)
}

async fn write_raw(stream: &mut OwnedWriteHalf, buf: &mut [u8], n: usize) -> Result<()> {
    buf[..4].copy_from_slice(&(n as u32).to_be_bytes());
    stream.write_all(&buf[..4 + n]).await
}

pub struct Reader {
    stream: OwnedReadHalf,
    noise: Arc<StatelessTransportState>,
    nonce: u64,
    wire: Vec<u8>,
    plain: Vec<u8>,
}

pub struct Writer {
    stream: OwnedWriteHalf,
    noise: Arc<StatelessTransportState>,
    nonce: u64,
    out: Vec<u8>,
    plain: Vec<u8>,
}

pub struct Link {
    rd: Reader,
    wr: Writer,
    peer: (u64, String),
}

impl Link {
    pub async fn connect(
        key: &[u8],
        addr: impl ToSocketAddrs,
        me: u64,
        my_addr: &str,
    ) -> Result<Link> {
        Self::start(key, TcpStream::connect(addr).await?, me, my_addr, true).await
    }

    pub async fn accept(key: &[u8], stream: TcpStream, me: u64, my_addr: &str) -> Result<Link> {
        Self::start(key, stream, me, my_addr, false).await
    }

    pub fn peer(&self) -> (u64, &str) {
        (self.peer.0, &self.peer.1)
    }

    pub fn split(self) -> (Reader, Writer) {
        (self.rd, self.wr)
    }

    pub async fn send(&mut self, msg: &Msg) -> Result<()> {
        self.wr.send_bytes(&encode(msg)).await
    }

    pub async fn recv(&mut self) -> Result<Msg> {
        decode(&self.rd.recv_bytes(MAX_FRAME).await?).map_err(bad)
    }

    async fn start(
        key: &[u8],
        stream: TcpStream,
        me: u64,
        my_addr: &str,
        initiator: bool,
    ) -> Result<Link> {
        stream.set_nodelay(true)?;
        let mut hs = handshake(LINK_LABEL, key, initiator)?;
        let mut rd = vec![0u8; NOISE_MAX];
        let mut wr = vec![0u8; 4 + NOISE_MAX];
        let (mut sr, mut sw) = stream.into_split();
        while !hs.is_handshake_finished() {
            if hs.is_my_turn() {
                let n = hs.write_message(&[], &mut wr[4..]).map_err(bad)?;
                write_raw(&mut sw, &mut wr, n).await?;
            } else {
                let n = read_raw(&mut sr, &mut rd).await?;
                hs.read_message(&rd[..n], &mut wr[4..]).map_err(bad)?;
            }
        }
        let noise = Arc::new(hs.into_stateless_transport_mode().map_err(bad)?);
        let mut link = Link {
            rd: Reader {
                stream: sr,
                noise: noise.clone(),
                nonce: 0,
                wire: rd,
                plain: vec![0u8; NOISE_MAX],
            },
            wr: Writer {
                stream: sw,
                noise,
                nonce: 0,
                out: wr,
                plain: Vec::with_capacity(CHUNK + 1),
            },
            peer: (0, String::new()),
        };
        if my_addr.len() > ADDR_MAX {
            return Err(bad("address too long"));
        }
        let mut hello = me.to_be_bytes().to_vec();
        hello.extend_from_slice(my_addr.as_bytes());
        link.wr.send_bytes(&hello).await?;
        let hello = link.rd.recv_bytes(HELLO_MAX).await?;
        let (id, addr) = hello
            .split_first_chunk::<8>()
            .ok_or_else(|| bad("short hello"))?;
        let addr = String::from_utf8(addr.to_vec()).map_err(bad)?;
        if addr.len() > ADDR_MAX || addr.parse::<SocketAddr>().is_err() {
            return Err(bad("bad peer address"));
        }
        link.peer = (u64::from_be_bytes(*id), addr);
        Ok(link)
    }
}

impl Writer {
    pub async fn send_bytes(&mut self, data: &[u8]) -> Result<()> {
        if data.len() > MAX_FRAME {
            return Err(bad("frame too large"));
        }
        let mut rest = data;
        loop {
            let (head, tail) = rest.split_at(rest.len().min(CHUNK));
            self.plain.clear();
            self.plain.push(u8::from(!tail.is_empty()));
            self.plain.extend_from_slice(head);
            let n = self
                .noise
                .write_message(self.nonce, &self.plain, &mut self.out[4..])
                .map_err(bad)?;
            self.nonce += 1;
            limit(WRITE, write_raw(&mut self.stream, &mut self.out, n)).await?;
            if tail.is_empty() {
                return Ok(());
            }
            rest = tail;
        }
    }
}

impl Reader {
    pub async fn recv_bytes(&mut self, max: usize) -> Result<Vec<u8>> {
        let mut frame = Vec::new();
        loop {
            let n = limit(IDLE, read_raw(&mut self.stream, &mut self.wire)).await?;
            let m = self
                .noise
                .read_message(self.nonce, &self.wire[..n], &mut self.plain)
                .map_err(bad)?;
            self.nonce += 1;
            if m == 0 || self.plain[0] > 1 {
                return Err(bad("bad chunk"));
            }
            if frame.len() + m - 1 > max {
                return Err(bad("frame too large"));
            }
            frame.extend_from_slice(&self.plain[1..m]);
            if self.plain[0] == 0 {
                return Ok(frame);
            }
        }
    }
}
