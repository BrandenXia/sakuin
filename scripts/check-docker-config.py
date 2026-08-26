#!/usr/bin/env python3
"""Require the Docker profile to mirror the documented default profile."""

from __future__ import annotations

import copy
import difflib
import json
from pathlib import Path
import sys
import tomllib


ROOT = Path(__file__).resolve().parents[1]


def load(name: str) -> dict[str, object]:
    with (ROOT / "config" / name).open("rb") as source:
        return tomllib.load(source)


expected = copy.deepcopy(load("sakuin.example.toml"))
expected["network"]["dht"]["bootstrap_file"] = (
    "/opt/sakuin/share/sakuin/dht-bootstrap.txt"
)
expected["storage"]["local_root"] = "/var/lib/sakuin"
expected["api"]["credential_store_directory"] = (
    "/var/lib/sakuin/operational/api"
)
expected["api"]["listen_address"] = "0.0.0.0"

actual = load("sakuin.docker.toml")
if actual != expected:
    expected_text = json.dumps(expected, indent=2, sort_keys=True).splitlines()
    actual_text = json.dumps(actual, indent=2, sort_keys=True).splitlines()
    print(
        "Docker configuration differs from the documented defaults:",
        file=sys.stderr,
    )
    print(
        "\n".join(
            difflib.unified_diff(
                expected_text,
                actual_text,
                fromfile="expected Docker profile",
                tofile="config/sakuin.docker.toml",
                lineterm="",
            )
        ),
        file=sys.stderr,
    )
    raise SystemExit(1)
