use std::io::ErrorKind;
use std::time::Duration;

use raft::eraftpb::{Entry, Message};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use verdandi_net::{Link, MAX_FRAME};
use verdandi_node::Msg;

const KEY: &[u8] = b"cluster key";

async fn pair(ka: &[u8], kb: &[u8]) -> (std::io::Result<Link>, std::io::Result<Link>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = async {
        let (stream, _) = listener.accept().await.unwrap();
        Link::accept(kb, stream, 2, "127.0.0.2:2").await
    };
    let client = Link::connect(ka, addr, 1, "127.0.0.1:1");
    let (s, c) = tokio::time::timeout(Duration::from_secs(20), async {
        tokio::join!(server, client)
    })
    .await
    .unwrap();
    (c, s)
}

fn big(n: usize) -> Msg {
    let entry = Entry {
        data: vec![7u8; n].into(),
        ..Entry::default()
    };
    Msg::Raft(Message {
        entries: vec![entry].into(),
        ..Message::default()
    })
}

fn size(msg: &Msg) -> usize {
    match msg {
        Msg::Raft(m) => m.entries[0].data.len(),
        _ => 0,
    }
}

async fn raw_peer(listener: &TcpListener) -> (TcpStream, snow::TransportState, Link) {
    let addr = listener.local_addr().unwrap();
    let raw = async {
        let mut s = TcpStream::connect(addr).await.unwrap();
        let psk: [u8; 32] = Sha256::digest([b"verdandi link v1".as_slice(), KEY].concat()).into();
        let mut hs = snow::Builder::new("Noise_NNpsk0_25519_ChaChaPoly_SHA256".parse().unwrap())
            .prologue(b"verdandi link v1")
            .unwrap()
            .psk(0, &psk)
            .unwrap()
            .build_initiator()
            .unwrap();
        let mut buf = [0u8; 65535];
        let n = hs.write_message(&[], &mut buf).unwrap();
        s.write_u32(n as u32).await.unwrap();
        s.write_all(&buf[..n]).await.unwrap();
        let len = tokio::io::AsyncReadExt::read_u32(&mut s).await.unwrap() as usize;
        let mut rd = vec![0u8; len];
        tokio::io::AsyncReadExt::read_exact(&mut s, &mut rd)
            .await
            .unwrap();
        hs.read_message(&rd, &mut buf).unwrap();
        let mut t = hs.into_transport_mode().unwrap();
        let mut hello = vec![0u8];
        hello.extend_from_slice(&9u64.to_be_bytes());
        hello.extend_from_slice(b"127.0.0.9:9");
        let n = t.write_message(&hello, &mut buf).unwrap();
        s.write_u32(n as u32).await.unwrap();
        s.write_all(&buf[..n]).await.unwrap();
        (s, t)
    };
    let server = async {
        let (stream, _) = listener.accept().await.unwrap();
        Link::accept(KEY, stream, 2, "127.0.0.2:2").await.unwrap()
    };
    let ((s, t), link) = tokio::join!(raw, server);
    (s, t, link)
}

#[tokio::test]
async fn messages_and_peer_ids_cross_both_ways() {
    let (a, b) = pair(KEY, KEY).await;
    let (mut a, mut b) = (a.unwrap(), b.unwrap());
    assert_eq!(a.peer(), (2, "127.0.0.2:2"));
    assert_eq!(b.peer(), (1, "127.0.0.1:1"));
    a.send(&Msg::Lose(5)).await.unwrap();
    assert!(matches!(b.recv().await.unwrap(), Msg::Lose(5)));
    b.send(&Msg::Fresh(6, 7)).await.unwrap();
    assert!(matches!(a.recv().await.unwrap(), Msg::Fresh(6, 7)));
}

#[tokio::test]
async fn a_5_mib_frame_arrives_whole() {
    let (a, b) = pair(KEY, KEY).await;
    let (mut a, mut b) = (a.unwrap(), b.unwrap());
    let n = 5 << 20;
    let msg = big(n);
    let (sent, got) = tokio::join!(a.send(&msg), b.recv());
    sent.unwrap();
    assert_eq!(size(&got.unwrap()), n);
}

#[tokio::test]
async fn a_wrong_key_fails_both_sides() {
    let (a, b) = pair(KEY, b"other key").await;
    assert!(a.is_err());
    assert!(b.is_err());
}

#[tokio::test]
async fn a_length_over_the_noise_limit_closes_the_link() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (mut s, _t, mut link) = raw_peer(&listener).await;
    assert_eq!(link.peer(), (9, "127.0.0.9:9"));
    s.write_u32(u32::MAX).await.unwrap();
    let e = link.recv().await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::InvalidData);
    assert_eq!(e.to_string(), "noise message too large");
}

#[tokio::test]
async fn chunks_over_max_frame_close_the_link() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (mut s, mut t, mut link) = raw_peer(&listener).await;
    let writer = async {
        let mut plain = vec![1u8; 65535 - 16];
        plain[0] = 1;
        let mut out = vec![0u8; 65535];
        for _ in 0..(MAX_FRAME / (plain.len() - 1) + 2) {
            let n = t.write_message(&plain, &mut out).unwrap();
            if s.write_u32(n as u32).await.is_err() || s.write_all(&out[..n]).await.is_err() {
                break;
            }
        }
    };
    let (_, e) = tokio::join!(writer, link.recv());
    let e = e.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::InvalidData);
    assert_eq!(e.to_string(), "frame too large");
}

async fn put(s: &mut TcpStream, bytes: &[u8]) {
    s.write_u32(bytes.len() as u32).await.unwrap();
    s.write_all(bytes).await.unwrap();
}

fn seal_chunk(t: &mut snow::TransportState, plain: &[u8]) -> Vec<u8> {
    let mut out = vec![0u8; 65535];
    let n = t.write_message(plain, &mut out).unwrap();
    out.truncate(n);
    out
}

#[tokio::test]
async fn a_replayed_chunk_closes_the_link() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (mut s, mut t, mut link) = raw_peer(&listener).await;
    let c = seal_chunk(&mut t, &[1, 5, 5]);
    put(&mut s, &c).await;
    put(&mut s, &c).await;
    let e = link.recv().await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::InvalidData);
    assert_ne!(e.to_string(), "bad chunk");
}

#[tokio::test]
async fn a_reordered_chunk_closes_the_link() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (mut s, mut t, mut link) = raw_peer(&listener).await;
    let first = seal_chunk(&mut t, &[1, 5, 5]);
    let second = seal_chunk(&mut t, &[0, 6, 6]);
    put(&mut s, &second).await;
    put(&mut s, &first).await;
    let e = link.recv().await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::InvalidData);
    assert_ne!(e.to_string(), "bad chunk");
}

#[tokio::test]
async fn a_flag_byte_above_1_closes_the_link() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (mut s, mut t, mut link) = raw_peer(&listener).await;
    let c = seal_chunk(&mut t, &[2, 5, 5]);
    put(&mut s, &c).await;
    let e = link.recv().await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::InvalidData);
    assert_eq!(e.to_string(), "bad chunk");
}

#[tokio::test]
async fn an_empty_chunk_closes_the_link() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (mut s, mut t, mut link) = raw_peer(&listener).await;
    let c = seal_chunk(&mut t, &[]);
    put(&mut s, &c).await;
    let e = link.recv().await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::InvalidData);
    assert_eq!(e.to_string(), "bad chunk");
}

async fn hello_with(addr: &str) -> (std::io::Result<Link>, std::io::Result<Link>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let at = listener.local_addr().unwrap();
    let server = async {
        let (stream, _) = listener.accept().await.unwrap();
        Link::accept(KEY, stream, 2, "127.0.0.2:2").await
    };
    tokio::join!(Link::connect(KEY, at, 1, addr), server)
}

#[tokio::test]
async fn a_hello_address_that_is_not_a_socket_address_closes_the_link() {
    let (c, s) = hello_with("not an address").await;
    assert!(c.is_ok());
    assert_eq!(s.err().unwrap().to_string(), "bad peer address");
    let long = format!("127.0.0.1:1{}", "0".repeat(300));
    let (c, _) = hello_with(&long).await;
    assert_eq!(c.err().unwrap().to_string(), "address too long");
}

#[tokio::test]
async fn a_slow_link_sends_a_large_frame_when_each_chunk_is_fast() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (mut s, mut t, mut link) = raw_peer(&listener).await;
    let n = 15 << 20;
    let reader = async {
        let mut total = 0;
        let mut buf = vec![0u8; 65535];
        let mut plain = vec![0u8; 65535];
        let mut first = true;
        loop {
            let len = s.read_u32().await.unwrap() as usize;
            s.read_exact(&mut buf[..len]).await.unwrap();
            let m = t.read_message(&buf[..len], &mut plain).unwrap();
            if std::mem::take(&mut first) {
                continue;
            }
            total += m - 1;
            if plain[0] == 0 {
                return total;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    let msg = big(n);
    let started = std::time::Instant::now();
    let (sent, total) = tokio::join!(link.send(&msg), reader);
    sent.unwrap();
    assert!(total >= n);
    assert!(started.elapsed() > Duration::from_secs(5));
}
