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
            # Parameterized routes are matched by prefix/suffix in the Worker,
            # while fixed routes appear verbatim.
            path = route["path"]
            static_prefix = path.split("{", 1)[0]
            self.assertIn(static_prefix or path, source)

    def test_contract_keeps_the_stream_terminal_events(self) -> None:
        repository_root = Path(__file__).resolve().parents[3]
        contract = json.loads(
            (repository_root / "protocol" / "http-contract.json").read_text(encoding="utf-8")
        )

        self.assertIn("done", contract["sse_events"])
        self.assertIn("error", contract["sse_events"])


if __name__ == "__main__":
    unittest.main()
