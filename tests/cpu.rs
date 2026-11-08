use super::*;

#[test]
fn shares_come_from_the_total_line_and_cpus_from_the_per_cpu_lines() {
    let stat = |user, idle| {
        format!(
            "cpu  {user} 0 10 {idle} 0 0 5 0 0 0\ncpu0 1 0 1 1 0 0 0 0 0 0\ncpu1 1 0 1 1 0 0 0 0 0 0\nintr 1\n"
        )
    };
    let now = Instant::now();
    let before = MachineSample::parse(&stat(100, 100), now).unwrap();
    let after = MachineSample::parse(&stat(150, 135), now).unwrap();
    let machine = before.until(after).unwrap();
    assert_eq!(machine.cpus, 2);
    // 50 user and 35 idle ticks: 85 in all, 35 of them unused.
    assert!((machine.busy_percent - 100.0 * 50.0 / 85.0).abs() < 1e-9);
    assert!((machine.busy_cpus - 2.0 * 50.0 / 85.0).abs() < 1e-9);
    assert!((machine.idle_percent - 100.0 * 35.0 / 85.0).abs() < 1e-9);
    assert!(after.until(before).is_none());
}
