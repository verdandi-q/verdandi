use std::time::Duration;

use raft::eraftpb::{Entry, Message};
use tokio::sync::mpsc::Receiver;
use tokio::time::{sleep, timeout};
use verdandi_net::{Net, Wire};
use verdandi_node::Msg;

const KEY: &[u8] = b"cluster key";
const BOUND: Duration = Duration::from_secs(10);

type Rx = Receiver<(u64, Wire, Msg)>;

const W: Wire = Wire {
    sent_ms: 5,
    version: 9,
};

async fn node(id: u64) -> (Net, Rx) {
    Net::start(KEY, "127.0.0.1:0", id).await.unwrap()
}

async fn until(what: &str, f: impl Fn() -> bool) {
    timeout(BOUND, async {
        while !f() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timeout: {what}"));
}

async fn got(rx: &mut Rx) -> (u64, Msg) {
    let (from, wire, msg) = timeout(BOUND, rx.recv()).await.unwrap().unwrap();
    assert_eq!(wire, W);
    (from, msg)
}

#[tokio::test]
async fn two_nets_deliver_both_ways() {
    let (a, mut ra) = node(1).await;
    let (b, mut rb) = node(2).await;
    a.connect(2, b.addr());
    a.send(2, Msg::Lose(5), W);
    assert!(matches!(got(&mut rb).await, (1, Msg::Lose(5))));
    until("b linked to a", || b.linked(1)).await;
    b.send(1, Msg::Lose(6), W);
    assert!(matches!(got(&mut ra).await, (2, Msg::Lose(6))));
}

#[tokio::test]
async fn a_relayable_msg_goes_through_b_when_a_blocks_c() {
    let (a, _ra) = node(1).await;
    let (b, _rb) = node(2).await;
    let (c, mut rc) = node(3).await;
    a.block(3);
    a.connect(2, b.addr());
    c.connect(2, b.addr());
    a.send(2, Msg::Lose(0), W);
    c.send(2, Msg::Lose(0), W);
    until("b linked to a and c", || b.linked(1) && b.linked(3)).await;
    a.send(3, Msg::Fresh(7, 8), W);
    assert!(matches!(got(&mut rc).await, (1, Msg::Fresh(7, 8))));
}

#[tokio::test]
async fn block_stops_a_working_direct_link_and_drops_a_non_relayable_msg() {
    let (a, _ra) = node(1).await;
    let (b, _rb) = node(2).await;
    let (c, mut rc) = node(3).await;
    a.connect(3, c.addr());
    a.send(3, Msg::Lose(1), W);
    assert!(matches!(got(&mut rc).await, (1, Msg::Lose(1))));
    a.connect(2, b.addr());
    c.connect(2, b.addr());
    a.send(2, Msg::Lose(0), W);
    c.send(2, Msg::Lose(0), W);
    until("b linked to a and c", || b.linked(1) && b.linked(3)).await;
    a.block(3);
    until("a closes the link to c", || !a.linked(3)).await;
    a.send(3, Msg::Lose(9), W);
    assert_eq!(a.dropped(), 1);
    a.send(3, Msg::Fresh(7, 8), W);
    assert!(matches!(got(&mut rc).await, (1, Msg::Fresh(7, 8))));
}

#[tokio::test]
async fn a_killed_link_reconnects_on_the_next_send() {
    let (a, _ra) = node(1).await;
    let (b, mut rb) = node(2).await;
    a.connect(2, b.addr());
    a.send(2, Msg::Lose(1), W);
    assert!(matches!(got(&mut rb).await, (1, Msg::Lose(1))));
    a.kill(2);
    a.send(2, Msg::Lose(2), W);
    assert!(matches!(got(&mut rb).await, (1, Msg::Lose(2))));
}

#[tokio::test]
async fn a_dial_that_reaches_the_wrong_id_is_dropped() {
    let (a, _ra) = node(1).await;
    let (n3, mut r3) = node(3).await;
    let (n4, mut r4) = node(4).await;
    a.connect(3, n4.addr());
    a.send(3, Msg::Lose(1), W);
    until("the failed dial drops the frame", || a.dropped() == 1).await;
    assert!(!a.linked(4) && !a.linked(3));
    a.connect(3, n3.addr());
    sleep(Duration::from_millis(300)).await;
    a.send(3, Msg::Lose(2), W);
    assert!(matches!(got(&mut r3).await, (1, Msg::Lose(2))));
    assert!(
        timeout(Duration::from_millis(100), r4.recv())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn simultaneous_dials_lose_no_frame() {
    for _ in 0..5 {
        let (a, mut ra) = node(1).await;
        let (b, mut rb) = node(2).await;
        a.connect(2, b.addr());
        b.connect(1, a.addr());
        for i in 0..20 {
            a.send(2, Msg::Lose(i), W);
            b.send(1, Msg::Lose(100 + i), W);
        }
        let mut at_b: Vec<u64> = Vec::new();
        let mut at_a: Vec<u64> = Vec::new();
        for _ in 0..20 {
            if let (1, Msg::Lose(n)) = got(&mut rb).await {
                at_b.push(n);
            }
            if let (2, Msg::Lose(n)) = got(&mut ra).await {
                at_a.push(n);
            }
        }
        at_a.sort();
        at_b.sort();
        assert_eq!(at_b, (0..20).collect::<Vec<_>>());
        assert_eq!(at_a, (100..120).collect::<Vec<_>>());
    }
}

#[tokio::test]
async fn an_oversize_frame_is_dropped_and_the_link_stays() {
    let (a, _ra) = node(1).await;
    let (b, mut rb) = node(2).await;
    a.connect(2, b.addr());
    let big = vec![((u64::MAX, u64::MAX), u64::MAX, u64::MAX); 500_000];
    a.send(2, Msg::Records(big, Default::default(), false), W);
    a.send(2, Msg::Lose(3), W);
    assert!(matches!(got(&mut rb).await, (1, Msg::Lose(3))));
    assert_eq!(a.dropped(), 1);
    assert!(a.linked(2));
}

#[tokio::test]
async fn a_full_queue_drops_the_oldest_and_counts_it() {
    let silent_peer = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (a, _ra) = node(1).await;
    a.connect(2, &silent_peer.local_addr().unwrap().to_string());
    for i in 0..1030 {
        a.send(2, Msg::Lose(i), W);
    }
    assert_eq!(a.dropped(), 6);
}

#[tokio::test]
async fn a_link_that_never_carries_a_frame_redials_with_backoff() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let accepts = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let counted = accepts.clone();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            if verdandi_net::Link::accept(KEY, stream, 2, "127.0.0.2:2")
                .await
                .is_ok()
            {
                counted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    });
    let (a, _ra) = node(1).await;
    a.connect(2, &addr);
    let entry = Entry {
        data: vec![7u8; 8 << 20].into(),
        ..Entry::default()
    };
    let big = Msg::Raft(Message {
        entries: vec![entry].into(),
        ..Message::default()
    });
    for _ in 0..30 {
        a.send(2, big.clone(), W);
        sleep(Duration::from_millis(50)).await;
    }
    let n = accepts.load(std::sync::atomic::Ordering::Relaxed);
    assert!(n <= 6, "{n} dials in 1.5 s");
}
