use std::{fs, path::PathBuf, process::Command};

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

fn temporary(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("trade-replay-{name}-{}", std::process::id()))
}

#[test]
fn golden_bytes_are_identical_in_fresh_processes() {
    let input = fixture("prevention-continuation.commands.jsonl");
    let expected = fs::read(fixture("prevention-continuation.events.jsonl")).unwrap();
    let first = temporary("first.events.jsonl");
    let second = temporary("second.events.jsonl");
    for output in [&first, &second] {
        let status = Command::new(env!("CARGO_BIN_EXE_trade-replay"))
            .arg(&input)
            .arg(output)
            .status()
            .unwrap();
        assert!(status.success());
    }
    assert_eq!(fs::read(&first).unwrap(), expected);
    assert_eq!(fs::read(&second).unwrap(), expected);
    let _ = fs::remove_file(first);
    let _ = fs::remove_file(second);
}

#[test]
fn malformed_input_never_replaces_existing_output() {
    let input = temporary("malformed.commands.jsonl");
    let output = temporary("preserved.events.jsonl");
    fs::write(&input, b"{\"format\":\"trade-rs-command-tape\",\"version\":1,\"symbols\":[\"XYZ\"]}\n{\"type\":\"limit\",\"symbol\":\"XYZ\",\"order_id\":1").unwrap();
    fs::write(&output, b"already-published\n").unwrap();
    let status = Command::new(env!("CARGO_BIN_EXE_trade-replay"))
        .arg(&input)
        .arg(&output)
        .status()
        .unwrap();
    assert!(!status.success());
    assert_eq!(fs::read(&output).unwrap(), b"already-published\n");
    let _ = fs::remove_file(input);
    let _ = fs::remove_file(output);
}
