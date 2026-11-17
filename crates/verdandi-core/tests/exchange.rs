use verdandi_core::cluster::view::{
    Body, Head, State, Update, View, encode_digest, encode_lost, encode_updates,
};

fn full_body(disk: u64, members: &[u64], holds: u64) -> Body {
    Body {
        address: format!("10.0.{}.{}:8080", disk / 250, disk % 250),
        join_ms: 1_759_000_000_000 + disk,
        alive_ms: 86_400_000,
        format_max: 1,
        committed: 1,
        zone: "eu-west-1a".to_owned(),
        spot_pool: "eu-west-1a/m7g.large".to_owned(),
        instance_type: "m7g.large".to_owned(),
        launch_type: "EC2".to_owned(),
        host: disk.wrapping_mul(0x9e37_79b9),
        prefix: "10.0.0.0/24".to_owned(),
        round_trips: members
            .iter()
            .filter(|m| **m != disk)
            .map(|m| (*m, 350))
            .collect(),
        disk_total: 20_000_000_000,
        disk_free: 10_000_000_000,
        fill_rate: 1_000_000,
        holds,
        debt: 0,
        count: 3,
        era: 1,
        epoch: 9,
        domains: 42,
        quiet: true,
        reconciled: true,
        marks: Default::default(),
        unreachable: vec![1, 2],
        signals: 0,
        remaining: 0,
        drained: true,
        can_found: false,
    }
}

fn exchange(n: u64, lost: u64) -> usize {
    let members: Vec<u64> = (1..=n).collect();
    let fill = |me: u64, holds: u64| {
        let mut view = View::new(me, 1, full_body(me, &members, holds), 0, 0);
        let updates: Vec<Update> = members
            .iter()
            .filter(|m| **m != me)
            .map(|m| Update {
                disk: *m,
                head: Head {
                    restart: 1,
                    heartbeat: 5,
                    state: State::Alive,
                },
                body: Some(((1, holds as u32), full_body(*m, &members, holds))),
            })
            .collect();
        view.merge(&updates, 0, 0);
        view
    };
    let a = fill(1, 2);
    let b = fill(2, 1);
    assert_eq!(
        a.answer(&b.digest())
            .iter()
            .filter(|u| u.body.is_some())
            .count(),
        members.len() - 1
    );
    let lost: Vec<(u64, u64)> = (1..=lost).map(|d| (d, 1)).collect();
    encode_digest(&b.digest()).len()
        + encode_updates(&a.answer(&b.digest())).len()
        + encode_digest(&a.digest()).len()
        + encode_updates(&b.answer(&a.digest())).len()
        + 2 * encode_lost(&lost).len()
}

#[test]
fn one_exchange_up_to_the_largest_cluster_fits_in_one_frame() {
    for members in [50, 150] {
        for lost in [0, members / 2] {
            let bytes = exchange(members, lost);
            println!("EXCHANGE BYTES members={members} lost={lost} bytes={bytes}");
            assert!(
                bytes < 65_536,
                "members={members} lost={lost} bytes={bytes}"
            );
        }
    }
}
