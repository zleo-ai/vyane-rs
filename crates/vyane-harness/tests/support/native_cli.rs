//! Standalone, std-only fake CLI compiled by native_cli.rs into a temp directory.

use std::io::Write as _;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let directory = std::env::var_os("VYANE_FIXTURE_DIR").expect("fixture directory");
    let directory = std::path::Path::new(&directory);
    let mut argv = std::fs::File::create(directory.join("argv.bin")).expect("argv capture");
    for arg in &args {
        argv.write_all(&(arg.len() as u64).to_le_bytes())
            .expect("length");
        argv.write_all(arg.as_bytes()).expect("argument");
    }
    std::fs::write(
        directory.join("cwd.txt"),
        std::env::current_dir()
            .expect("cwd")
            .to_string_lossy()
            .as_bytes(),
    )
    .expect("cwd capture");
    // Deliberately use CRLF for both protocols, even on Unix.
    if args.iter().any(|arg| arg == "--output-format") {
        print!(
            "{{\"type\":\"result\",\"result\":\"fixture final\",\"session_id\":\"fixture-session\",\"usage\":{{\"input_tokens\":2,\"output_tokens\":3}}}}\r\n"
        );
    } else {
        let output = args
            .windows(2)
            .find(|pair| pair[0] == "-o")
            .expect("last message path");
        std::fs::write(&output[1], "fixture final\r\n").expect("last message");
        print!("{{\"type\":\"thread.started\",\"thread_id\":\"fixture-session\"}}\r\n");
        print!(
            "{{\"type\":\"item.completed\",\"item\":{{\"type\":\"agent_message\",\"text\":\"fixture final\"}}}}\r\n"
        );
        print!(
            "{{\"type\":\"turn.completed\",\"usage\":{{\"input_tokens\":2,\"output_tokens\":3}}}}\r\n"
        );
    }
}
