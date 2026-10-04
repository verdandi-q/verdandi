pub const MAX_NAME_BYTES: usize = 200;

const DEAD_INFIX: &str = ".dead.";

fn valid(name: &str, star: bool) -> bool {
    name.split('.').all(|part| {
        !part.is_empty()
            && part
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-' || (star && b == b'*'))
    })
}

pub fn valid_name(name: &str) -> bool {
    let short = match name.split_once(DEAD_INFIX) {
        Some((origin, group)) => {
            origin.len() <= MAX_NAME_BYTES && group.len() <= MAX_NAME_BYTES && !is_dead(group)
        }
        None => name.len() <= MAX_NAME_BYTES,
    };
    short && valid(name, false) && name.rsplit('.').next() != Some("dead")
}

pub fn valid_group(name: &str) -> bool {
    valid_name(name) && !is_dead(name)
}

pub fn valid_pattern(pattern: &str) -> bool {
    pattern.len() <= MAX_NAME_BYTES && valid(pattern, true)
}

pub fn publishable(name: &str) -> bool {
    valid_name(name) && !name.starts_with('_') && !is_dead(name)
}

pub fn is_dead(stream: &str) -> bool {
    stream.contains(DEAD_INFIX)
}

pub fn origin(stream: &str) -> &str {
    stream.split(DEAD_INFIX).next().unwrap_or(stream)
}

pub fn dead_stream(stream: &str, group: &str) -> String {
    format!("{}{DEAD_INFIX}{group}", origin(stream))
}

pub fn matches(selector: &str, stream: &str) -> bool {
    selector == stream || (!stream.starts_with('_') && !is_dead(stream) && glob(selector, stream))
}

fn glob(pattern: &str, text: &str) -> bool {
    let (p, t) = (pattern.as_bytes(), text.as_bytes());
    let (mut pi, mut ti) = (0, 0);
    let mut star: Option<(usize, usize)> = None;
    while ti < t.len() {
        if pi < p.len() && p[pi] == b'*' {
            star = Some((pi, ti));
            pi += 1;
        } else if pi < p.len() && p[pi] == t[ti] {
            pi += 1;
            ti += 1;
        } else if let Some((sp, st)) = star {
            pi = sp + 1;
            ti = st + 1;
            star = Some((sp, st + 1));
        } else {
            return false;
        }
    }
    p[pi..].iter().all(|&b| b == b'*')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_dot_separated_parts_of_the_allowed_bytes() {
        for good in [
            "orders",
            "orders.eu",
            "a-b_c.D9",
            "dead.x",
            &"x".repeat(200),
        ] {
            assert!(valid_name(good), "{good}");
        }
        for bad in [
            "",
            ".a",
            "a.",
            "a..b",
            "a b",
            "a/b",
            "a*",
            "ö",
            &"x".repeat(201),
        ] {
            assert!(!valid_name(bad), "{bad}");
        }
    }

    #[test]
    fn a_dead_stream_name_holds_a_full_stream_name_and_a_full_group_name() {
        let long = dead_stream(&"s".repeat(200), &"g".repeat(200));
        assert_eq!(long.len(), 406);
        assert!(valid_name(&long));
        assert!(!valid_name(&format!("{}.dead.g", "s".repeat(201))));
        assert!(!valid_name(&format!("s.dead.{}", "g".repeat(201))));
        assert!(!publishable(&long));
    }

    #[test]
    fn a_dead_stream_name_whose_group_part_holds_a_dead_part_is_refused() {
        assert!(!valid_name("a.dead.b.dead.c"));
        assert!(!valid_name("a.b.dead.c.dead.d.e"));
        assert!(valid_name("a.b.dead.c.d"));
    }

    #[test]
    fn a_name_whose_last_part_is_dead_is_refused() {
        for bad in ["dead", "x.dead", "jobs.dead", "a.b.dead"] {
            assert!(!valid_name(bad), "{bad}");
            assert!(!publishable(bad), "{bad}");
        }
        assert!(valid_name("jobs.dead.g"));
    }

    #[test]
    fn a_group_name_is_at_most_200_bytes_and_has_no_dead_part() {
        for good in ["billing", "orders.deadline", "dead.x", &"g".repeat(200)] {
            assert!(valid_group(good), "{good}");
        }
        let long = format!("{}.dead.{}", "s".repeat(200), "g".repeat(200));
        for bad in [
            "",
            "dead",
            "x.dead",
            "a.dead.b",
            "a b",
            &"g".repeat(201),
            &long,
        ] {
            assert!(!valid_group(bad), "{bad}");
        }
    }

    #[test]
    fn patterns_allow_a_star_in_any_part() {
        assert!(valid_pattern("orders.*"));
        assert!(valid_pattern("*"));
        assert!(valid_pattern("a*b.c"));
        assert!(!valid_pattern("orders.?"));
        assert!(!valid_pattern("orders..*"));
    }

    #[test]
    fn client_publish_refuses_system_and_dead_names() {
        assert!(publishable("orders"));
        assert!(publishable("orders.deadline"));
        assert!(!publishable("_auth"));
        assert!(!publishable("orders.dead.billing"));
        assert!(!publishable("bad name"));
    }

    #[test]
    fn a_star_matches_any_bytes_also_dots() {
        assert!(matches("orders.*", "orders.eu"));
        assert!(matches("orders.*", "orders.eu.west"));
        assert!(matches("*", "orders"));
        assert!(matches("o*s", "orders"));
        assert!(matches("*.eu", "orders.eu"));
        assert!(!matches("orders.*", "orders"));
        assert!(!matches("orders.*", "payments.eu"));
        assert!(!matches("o*x", "orders"));
    }

    #[test]
    fn a_pattern_never_matches_a_dead_or_system_stream() {
        assert!(!matches("*", "orders.dead.billing"));
        assert!(!matches("orders.*", "orders.dead.billing"));
        assert!(!matches("*", "_auth"));
        assert!(matches("orders.dead.billing", "orders.dead.billing"));
        assert!(matches("_auth", "_auth"));
    }

    #[test]
    fn a_dead_copy_of_a_dead_copy_keeps_the_first_dead_stream() {
        assert_eq!(dead_stream("orders", "billing"), "orders.dead.billing");
        assert_eq!(
            dead_stream("orders.dead.billing", "billing"),
            "orders.dead.billing"
        );
        assert_eq!(
            dead_stream("orders.dead.billing", "audit"),
            "orders.dead.audit"
        );
        assert_eq!(origin("orders.dead.billing"), "orders");
        assert_eq!(origin("orders"), "orders");
    }
}
