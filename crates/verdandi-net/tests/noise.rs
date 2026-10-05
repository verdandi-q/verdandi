const PARAMS: &str = "Noise_NNpsk0_25519_ChaChaPoly_SHA256";

#[test]
fn nnpsk0_handshake_completes_with_32_byte_psk() {
    let psk = [7u8; 32];
    let params = PARAMS.parse().unwrap();
    let mut initiator = snow::Builder::new(params)
        .psk(0, &psk)
        .unwrap()
        .build_initiator()
        .unwrap();
    let mut responder = snow::Builder::new(PARAMS.parse().unwrap())
        .psk(0, &psk)
        .unwrap()
        .build_responder()
        .unwrap();
    let mut wire = [0u8; 256];
    let mut out = [0u8; 256];

    let n = initiator.write_message(b"", &mut wire).unwrap();
    responder.read_message(&wire[..n], &mut out).unwrap();
    let n = responder.write_message(b"", &mut wire).unwrap();
    initiator.read_message(&wire[..n], &mut out).unwrap();

    assert!(initiator.is_handshake_finished());
    assert!(responder.is_handshake_finished());
    let mut a = initiator.into_transport_mode().unwrap();
    let mut b = responder.into_transport_mode().unwrap();
    let n = a.write_message(b"ping", &mut wire).unwrap();
    let n = b.read_message(&wire[..n], &mut out).unwrap();
    assert_eq!(&out[..n], b"ping");
}
