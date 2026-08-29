from __future__ import annotations

import json
import unittest
from pathlib import Path


class ProtocolContractTests(unittest.TestCase):
    def test_worker_exposes_every_contract_route(self) -> None:
        repository_root = Path(__file__).resolve().parents[3]
        contract = json.loads(
            (repository_root / "protocol" / "http-contract.json").read_text(encoding="utf-8")
        )
        source = (repository_root / "implementations" / "python" / "server.py").read_text(
            encoding="utf-8"
        )

        for route in contract["routes"]:
            self.assertIn(route["path"], source)

    def test_contract_keeps_the_stream_terminal_events(self) -> None:
        repository_root = Path(__file__).resolve().parents[3]
        contract = json.loads(
            (repository_root / "protocol" / "http-contract.json").read_text(encoding="utf-8")
        )

        self.assertIn("done", contract["sse_events"])
        self.assertIn("error", contract["sse_events"])


if __name__ == "__main__":
    unittest.main()
