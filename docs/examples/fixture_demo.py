#!/usr/bin/env python3
"""Exercise the installed CLI against local scripted responses, not real models."""

import argparse
import json
import os
import subprocess
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


class FixtureHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        failed = self.path.startswith("/primary/")
        body = (
            {"error": {"message": "scripted primary failure", "type": "fixture"}}
            if failed
            else {
                "id": "fixture",
                "object": "chat.completion",
                "model": request["model"],
                "choices": [{"index": 0, "message": {
                    "role": "assistant", "content": "fixture: " + request["model"]
                }, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            }
        )
        payload = json.dumps(body).encode()
        self.send_response(500 if failed else 200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *_args):
        pass


def run_demo(binary, server, directory):
    config = directory / "config.toml"
    port = server.server_address[1]
    config.write_text(f'''[providers.fixture_demo]
base_url = "http://127.0.0.1:{port}/demo/v1"
auth_style = "bearer"
protocol = "openai_chat"
default_model = "chat-model"
[providers.fixture_primary]
base_url = "http://127.0.0.1:{port}/primary/v1"
auth_style = "bearer"
protocol = "openai_chat"
default_model = "primary-model"
[profiles.fixture_chat]
provider = "fixture_demo"
harness = "none"
model = "chat-model"
[profiles.fixture_review]
provider = "fixture_demo"
harness = "none"
model = "review-model"
[profiles.fixture_resilient]
provider = "fixture_primary"
harness = "none"
model = "primary-model"
failover = ["fixture_review"]
''')
    env = os.environ.copy()
    env["VYANE_DATA_DIR"] = str(directory / "data")
    cases = [
        ("dispatch", ["dispatch", "Explain a Rust trait", "--target", "fixture_chat"]),
        ("broadcast", ["broadcast", "Compare an enum and a trait", "--targets",
                       "fixture_chat,fixture_review"]),
        ("failover", ["dispatch", "Explain a Rust trait", "--target", "fixture_resilient"]),
    ]
    for name, args in cases:
        result = subprocess.run(
            [str(binary), "--config", str(config), *args, "--json"],
            capture_output=True, text=True, env=env, timeout=30, check=True,
        )
        parsed = json.loads(result.stdout)
        if name == "broadcast":
            assert [row["target"] for row in parsed] == ["fixture_chat", "fixture_review"]
            assert [row["output"] for row in parsed] == ["fixture: chat-model", "fixture: review-model"]
            assert all(row["record"]["status"] == "success" for row in parsed)
            print("broadcast: 2 separate successful results (chat-model, review-model)")
        else:
            attempts = len(parsed["record"]["attempts"])
            assert parsed["record"]["status"] == "success"
            assert attempts == (2 if name == "failover" else 1)
            expected = "fixture: review-model" if name == "failover" else "fixture: chat-model"
            assert parsed["output"] == expected
            print(f"{name}: {expected}; recorded attempts={attempts}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("target/install/bin/vyane"))
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    # Resolve the temporary root so macOS's /var -> /private/var alias does not
    # introduce a symlink into a path checked by the local storage boundary.
    temporary_root = Path(tempfile.gettempdir()).resolve()
    server = ThreadingHTTPServer(("127.0.0.1", 0), FixtureHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        print("Local HTTP fixtures only; no live model calls.")
        with tempfile.TemporaryDirectory(prefix="vyane-demo-", dir=temporary_root) as temp:
            run_demo(binary, server, Path(temp))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


if __name__ == "__main__":
    main()
