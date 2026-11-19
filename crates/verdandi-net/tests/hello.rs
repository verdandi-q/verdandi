use sha2::{Digest, Sha256};
use verdandi_net::{HELLO_MAX, Hello, key_ok, open, seal};

const KEY: &[u8] = b"cluster key";

fn hello() -> Hello {
    Hello {
        node: 7,
        addr: "10.0.0.7:7000".into(),
        incarnation: 3,
        sent_ms: 1_700_000_000_000,
        cluster: None,
    }
}

#[test]
fn round_trip() {
    assert_eq!(open(KEY, &seal(KEY, &hello()).unwrap()).unwrap(), hello());
}

#[test]
fn wrong_key_fails() {
    assert!(open(b"other key", &seal(KEY, &hello()).unwrap()).is_err());
}

#[test]
fn changed_byte_fails() {
    let sealed = seal(KEY, &hello()).unwrap();
    for i in 0..sealed.len() {
        let mut b = sealed.clone();
        b[i] ^= 1;
        assert!(open(KEY, &b).is_err(), "byte {i}");
    }
}

#[test]
fn two_seals_differ() {
    assert_ne!(seal(KEY, &hello()).unwrap(), seal(KEY, &hello()).unwrap());
}

#[test]
fn oversize_and_short_fail() {
    assert!(open(KEY, &vec![0u8; HELLO_MAX + 1]).is_err());
    assert!(open(KEY, &[]).is_err());
    assert!(open(KEY, &[0u8; 10]).is_err());
}

#[test]
fn a_long_address_is_refused_both_ways() {
    let mut h = hello();
    h.addr = "a".repeat(256);
    assert!(seal(KEY, &h).is_err());
    h.addr = "a".repeat(100_000);
    assert!(seal(KEY, &h).is_err());
    h.addr = "a".repeat(255);
    assert_eq!(open(KEY, &seal(KEY, &h).unwrap()).unwrap(), h);
    h.addr = "a".repeat(256);
    let payload = postcard::to_allocvec(&h).unwrap();
    let mut hs = noise(b"verdandi hello v1");
    let mut out = vec![0u8; 2048];
    let n = hs.write_message(&payload, &mut out).unwrap();
    assert!(open(KEY, &out[..n]).is_err());
}

fn noise(label: &[u8]) -> snow::HandshakeState {
    let psk: [u8; 32] = Sha256::digest([label, KEY].concat()).into();
    snow::Builder::new("Noise_NNpsk0_25519_ChaChaPoly_SHA256".parse().unwrap())
        .prologue(label)
        .unwrap()
        .psk(0, &psk)
        .unwrap()
        .build_initiator()
        .unwrap()
}

#[test]
fn a_link_label_key_cannot_open_a_hello() {
    let payload = postcard::to_allocvec(&hello()).unwrap();
    let mut out = vec![0u8; 2048];
    let n = noise(b"verdandi link v1")
        .write_message(&payload, &mut out)
        .unwrap();
    assert!(open(KEY, &out[..n]).is_err());
    let n = noise(b"verdandi hello v1")
        .write_message(&payload, &mut out)
        .unwrap();
    assert_eq!(open(KEY, &out[..n]).unwrap(), hello());
}

#[test]
fn key_ok_refuses_a_short_key() {
    assert!(key_ok(&[0u8; 31]).is_err());
    assert!(key_ok(&[0u8; 32]).is_ok());
}
