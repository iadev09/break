use super::*;

const URL: &str = "http://example.test/plain";

fn parsed(arguments: &[&str]) -> Result<Cli, clap::Error> {
    parse(arguments.iter().map(OsString::from))
}

#[test]
fn parses_numeric_concurrency_without_rate_units_or_warmup() {
    let base = [
        "brk",
        URL,
        "--conn",
        "4",
        "--phase",
        "10s",
        "--pace",
        "100",
        "--timeout",
        "2s",
    ];
    let mut arguments = base.to_vec();
    arguments.extend(["--start", "500", "--sla", "99.99", "--max", "1000"]);
    let Test::Requests(args) = parsed(&arguments).unwrap().test else {
        panic!("an unnamed test is the request test");
    };
    assert_eq!(args.conn, 4);
    assert_eq!(args.start, Some(500));
    assert_eq!(args.pace, 100);
    assert_eq!(args.max_concurrency, Some(1_000));
    for unsupported in ["--warmup", "--tls", "--proto", "-t", "--max-rate"] {
        let mut arguments = base.to_vec();
        arguments.extend([unsupported, "1"]);
        assert!(parsed(&arguments).is_err());
    }
    assert!(parsed(&["brk", URL]).is_err());
}

#[test]
fn each_test_has_its_name_its_short_form_and_only_its_own_options() {
    let timing = ["--phase", "1s", "--timeout", "1s"];
    for name in ["requests", "req"] {
        let mut arguments =
            vec!["brk", name, URL, "--conn", "4", "--pace", "1"];
        arguments.extend(timing);
        assert!(matches!(parsed(&arguments).unwrap().test, Test::Requests(_)));
        arguments.extend(["--requests", "1"]);
        assert!(parsed(&arguments).is_err(), "{name}");
    }
    for name in ["connections", "conn"] {
        let mut arguments =
            vec!["brk", name, URL, "--pace", "8", "--requests", "3"];
        arguments.extend(timing);
        let Test::Connections(args) = parsed(&arguments).unwrap().test else {
            panic!("{name} selects the connection test");
        };
        assert_eq!((args.start, args.pace, args.requests), (None, 8, 3));
        arguments.extend(["--conn", "4"]);
        assert!(parsed(&arguments).is_err(), "{name}");
    }
}
