#!/usr/bin/env python3
"""Render BENCHMARKS.md tables from mqtt_compare JSONL.

Cells (status=ok clients only are compared; best value per metric is bold):
  msg/s (highest) · CPU ns/msg (lowest) [· p50 / p99 / p99.9 µs (lowest each) when present]
  serial latency rows: p50 / p99 µs (lowest each)

Usage:
  python3 benches/render_benchmarks.py
  python3 benches/render_benchmarks.py \\
    --mock DIR --mosquitto DIR --mosquitto-stock DIR --out BENCHMARKS.md
"""

from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path

CLIENTS = ("glide", "rumqttc", "mqtt5")
PAYLOAD = 256

# scenario -> (title, kind). kind: "load" or "lat"
SCENARIOS = (
    ("outbound_concurrent", "Outbound concurrent (callers sharing one client)"),
    ("outbound_spawn", "Outbound spawn (one task per message, 50k at once)"),
    ("outbound_rate", "Outbound rate (open loop, offered msg/s)"),
    ("outbound_pipelined", "Outbound pipelined (window 64, one task)"),
    ("outbound_serial_latency", "Outbound serial latency (idle round trip)"),
    ("ingress_load", "Ingress load"),
    ("ingress_serial", "Ingress serial latency (idle)"),
)
OUTBOUND = {
    "outbound_concurrent",
    "outbound_spawn",
    "outbound_rate",
    "outbound_pipelined",
    "outbound_serial_latency",
}
LEVEL_LABEL = {
    "outbound_concurrent": lambda lv: f"{lv} callers",
    "outbound_rate": lambda lv: f"{thin_int(lv)} msg/s offered",
}

MARKER_START = "<!-- benchmarks:generated:start -->"
MARKER_END = "<!-- benchmarks:generated:end -->"

NBSP = "\u202f"  # narrow no-break space for thousands


def thin_int(value: float) -> str:
    return f"{value:,.0f}".replace(",", NBSP)


def load_rows(run_dir: Path) -> list[dict]:
    rows: list[dict] = []
    for path in sorted(run_dir.glob("*.jsonl")):
        for line in path.read_text().splitlines():
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def index_rows(rows: list[dict]) -> dict:
    """(scenario, qos, payload, level, ack) -> client -> row."""
    out: dict = defaultdict(dict)
    for row in rows:
        key = (row["scenario"], row["qos"], row["payload"], row.get("level"), row.get("ack"))
        out[key][row["client"]] = row
    return out


def reason_label(row: dict) -> str:
    reason = row.get("reason") or "unsupported"
    return f"n/a (`{reason}`)"


def winners(values: list[tuple[str, float]], best) -> set[str]:
    if not values:
        return set()
    target = best(v for _, v in values)
    return {c for c, v in values if v == target}


def fmt_us(value: float) -> str:
    return f"{value:,.1f}".replace(",", NBSP) if value < 100 else thin_int(value)


def cell(row: dict | None, ok: dict[str, dict], client: str) -> str:
    """One table cell; `ok` maps client -> row for status=ok clients in the row."""
    if row is None:
        return "—"
    if row.get("status") != "ok":
        return reason_label(row)

    def best(key: str, fn, fmt) -> str:
        vals = [(c, r[key]) for c, r in ok.items() if key in r]
        text = fmt(row[key])
        return f"**{text}**" if client in winners(vals, fn) else text

    parts = []
    if "msg_s" in row:
        parts.append(best("msg_s", max, thin_int))
        parts.append(best("cpu_ns_per_msg", min, thin_int))
        if "p50_us" in row:
            lat = " / ".join(
                best(k, min, fmt_us) for k in ("p50_us", "p99_us", "p999_us") if k in row
            )
            parts.append(lat + " µs")
        return " · ".join(parts)
    # serial latency rows
    return " / ".join(best(k, min, fmt_us) for k in ("p50_us", "p99_us") if k in row)


def row_cells(clients: dict) -> str:
    ok = {c: clients[c] for c in CLIENTS if c in clients and clients[c].get("status") == "ok"}
    return " | ".join(cell(clients.get(c), ok, c) for c in CLIENTS)


def table(by: dict, scenario: str, qos: int, ack: str | None) -> str:
    levels = sorted(
        {k[3] for k in by if k[0] == scenario and k[1] == qos and k[2] == PAYLOAD and k[4] == ack},
        key=lambda x: (x is None, x),
    )
    if not levels:
        return ""
    first = "Level" if levels != [None] else "Payload"
    lines = [f"| {first} | glide | rumqttc | mqtt5 |", "|---:|---:|---:|---:|"]
    for lv in levels:
        clients = by.get((scenario, qos, PAYLOAD, lv, ack), {})
        label = LEVEL_LABEL[scenario](lv) if lv is not None else f"{PAYLOAD} B"
        lines.append(f"| {label} | {row_cells(clients)} |")
    return "\n".join(lines)


def units(scenario: str) -> str:
    if scenario in ("outbound_serial_latency", "ingress_serial"):
        return "Units: p50 / p99 µs."
    if scenario == "outbound_spawn":
        return "Units: msg/s · CPU ns/msg (no latency: all tasks start at once)."
    if scenario in ("outbound_concurrent", "outbound_rate"):
        return "Units: msg/s · CPU ns/msg · p50 / p99 / p99.9 µs per publish."
    return "Units: msg/s · CPU ns/msg."


def render_broker_block(by: dict, *, omit_outbound_qos0: bool) -> str:
    parts: list[str] = []
    for scenario, title in SCENARIOS:
        sec: list[str] = []
        qos_list = (1,) if omit_outbound_qos0 and scenario in OUTBOUND else (0, 1)
        for qos in qos_list:
            acks = ("after_enqueue", "before") if scenario.startswith("ingress") and qos == 1 else (None,)
            for ack in acks:
                body = table(by, scenario, qos, ack)
                if body:
                    tag = f" · `{ack}`" if ack else ""
                    sec += [f"#### QoS {qos}{tag}", "", body, ""]
        if sec:
            parts += [f"### {title}", "", units(scenario), ""] + sec
    return "\n".join(parts).rstrip() + "\n"


HEADLINE = (
    ("Outbound concurrent, 64 callers", "outbound_concurrent", 64),
    ("Outbound concurrent, 512 callers", "outbound_concurrent", 512),
    ("Outbound spawn", "outbound_spawn", None),
    ("Outbound rate, 50 000 msg/s offered", "outbound_rate", 50000),
    ("Outbound pipelined", "outbound_pipelined", None),
)


def render_headline(by: dict, qos: int) -> str:
    lines = ["| Scenario | glide | rumqttc | mqtt5 |", "|---|---:|---:|---:|"]
    for label, scenario, lv in HEADLINE:
        clients = by.get((scenario, qos, PAYLOAD, lv, None), {})
        lines.append(f"| {label} | {row_cells(clients)} |")
    return "\n".join(lines)


def render_generated(mock: dict, mosq: dict, stock: dict) -> str:
    bold = (
        "**Bold** = highest msg/s, lowest CPU ns/msg, lowest p50 / p99 / p99.9 among "
        "`status: ok` clients in that row (ties all bold)."
    )
    return "\n".join(
        [
            MARKER_START,
            "",
            "## Headline: QoS 1, 256 B, tuned Mosquitto",
            "",
            bold,
            "",
            render_headline(mosq, 1),
            "",
            "---",
            "",
            "## Mosquitto (tuned: unlimited inflight / queue)",
            "",
            "Broker: eclipse-mosquitto `2.1.2-alpine` via testcontainers (anonymous; `set_tcp_nodelay`; `max_inflight_messages 0`, `max_queued_messages 0`; no persistence / `$SYS` / logs).",
            "",
            bold,
            "",
            "Outbound QoS 0 is omitted: no wire counter without a measuring subscriber; see the mock tables.",
            "",
            render_broker_block(mosq, omit_outbound_qos0=True),
            "",
            "---",
            "",
            "## Mosquitto (stock limits)",
            "",
            "Broker: same image with Mosquitto's defaults, `max_inflight_messages 20` / `max_queued_messages 1000`. The broker advertises Receive Maximum 20 in CONNACK, so a client may have at most 20 unacknowledged QoS 1 PUBLISH packets in flight. QoS 1 only.",
            "",
            bold,
            "",
            render_broker_block(stock, omit_outbound_qos0=True),
            "",
            "---",
            "",
            "## Mock ceiling",
            "",
            "Broker: out-of-process mock MQTT 5 (`MQTT_COMPARE_BROKER=mock`): multi-thread Tokio, Arc fanout, `TCP_NODELAY`, inbound PUBLISH counter, `receive_max` 1024.",
            "Use for client CPU / enqueue ceiling attribution, not as a broker product comparison.",
            "",
            bold,
            "",
            render_broker_block(mock, omit_outbound_qos0=False),
            "",
            "---",
            "",
            "## Refresh",
            "",
            "```shell",
            "# one run per broker, pinned (see benches/README.md)",
            "for b in mock mosquitto mosquitto-stock; do",
            "  q=1; [ $b = mock ] && q=0,1",
            "  MQTT_COMPARE_BROKER=$b MQTT_COMPARE_QOS=$q \\",
            "  MQTT_COMPARE_BROKER_CPUS=2-3 MQTT_COMPARE_PUBLISHER_CPUS=2-3 \\",
            "  MQTT_COMPARE_RUNS=5 MQTT_COMPARE_OUT=target/mqtt_compare/full-$b \\",
            "    taskset -c 0-1 cargo bench --bench mqtt_compare",
            "done",
            "",
            "# rebuild the tables above",
            "python3 benches/render_benchmarks.py",
            "```",
            "",
            MARKER_END,
            "",
        ]
    )


def patch_file(path: Path, generated: str) -> None:
    text = path.read_text()
    if MARKER_START in text and MARKER_END in text:
        pattern = re.compile(
            re.escape(MARKER_START) + r".*?" + re.escape(MARKER_END),
            re.DOTALL,
        )
        new_text = pattern.sub(lambda _: generated.strip(), text, count=1)
        if not new_text.endswith("\n"):
            new_text += "\n"
        path.write_text(new_text)
        return

    # First run: keep preamble through caveats, replace from Highlights onward.
    cut = text.find("## Highlights")
    if cut == -1:
        cut = text.find("---\n\n## Highlights")
    if cut == -1:
        raise SystemExit(f"no markers and no ## Highlights in {path}")
    # Prefer cutting at the --- before Highlights if present
    dash = text.rfind("\n---\n", 0, cut)
    if dash != -1:
        cut = dash + 1
    preamble = text[:cut].rstrip() + "\n\n"
    path.write_text(preamble + generated)


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    base = root / "target/mqtt_compare"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mock", type=Path, default=base / "full-mock")
    parser.add_argument("--mosquitto", type=Path, default=base / "full-mosquitto")
    parser.add_argument(
        "--mosquitto-stock", type=Path, default=base / "full-mosquitto-stock"
    )
    parser.add_argument("--out", type=Path, default=root / "BENCHMARKS.md")
    args = parser.parse_args()

    mock, mosq, stock = (
        index_rows(load_rows(d)) for d in (args.mock, args.mosquitto, args.mosquitto_stock)
    )
    patch_file(args.out, render_generated(mock, mosq, stock))
    print(f"wrote tables into {args.out}")


if __name__ == "__main__":
    main()
