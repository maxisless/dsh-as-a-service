#!/usr/bin/env python3
"""Tiny OpenAI-compatible SSE server for local DSH integration tests."""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/chat/completions":
            self.send_error(404)
            return
        length = int(self.headers.get("Content-Length", "0"))
        body = json.loads(self.rfile.read(length))
        messages = body.get("messages", [])
        text = " \n".join(
            str(message.get("content", ""))
            for message in messages
            if isinstance(message, dict)
        )
        answer = "Whale" if "codename is Whale" in text and "What is my codename" in text else "Mock DSH response"
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        frames = [
            {"choices": [{"delta": {"role": "assistant", "content": None}}]},
            {"choices": [{"delta": {"content": answer}}]},
            {"choices": [{"delta": {}, "finish_reason": "stop"}], "usage": {"prompt_tokens": 10, "completion_tokens": 2}},
        ]
        for frame in frames:
            self.wfile.write(f"data: {json.dumps(frame)}\n\n".encode())
        self.wfile.write(b"data: [DONE]\n\n")

    def log_message(self, _fmt: str, *_args: object) -> None:
        pass


if __name__ == "__main__":
    ThreadingHTTPServer(("127.0.0.1", 18765), Handler).serve_forever()
