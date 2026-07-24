#!/usr/bin/env python3
"""Render BENCHMARKS.md tables from mqtt_compare JSONL.

Bold rules (per payload row, among status=ok clients only):
  load: highest msg/s and lowest cpu_ns_per_msg
  latency: lowest p50_us and lowest p99_us

Usage:
  python3 benches/render_benchmarks.py
  python3 benches/render_benchmarks.py \\
    --mosquitto target/mqtt_compare/full-mosquitto-100 \\
    --mock target/mqtt_compare/full-mock-100 \\
    --out BENCHMARKS.md
"""

from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path

CLIENTS = ("glide", "rumqttc", "mqtt5")
PAYLOADS = (64, 256, 1024, 4096)

LOAD_SCENARIOS = (
    "ingress_load",
    "outbound_serial",
    "outbound_pipelined",
    "outbound_concurrent",
)
LATENCY_SCENARIOS = (
    "ingress_serial",
    "outbound_serial_latency",
)

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
    """(scenario, qos, payload, ack) -> client -> row."""
    out: dict = defaultdict(dict)
    for row in rows:
        key = (row["scenario"], row["qos"], row["payload"], row.get("ack"))
        out[key][row["client"]] = row
    return out


def reason_label(row: dict) -> str:
    reason = row.get("reason") or "unsupported"
    return f"n/a (`{reason}`)"


def fmt_load_cell(row: dict | None, bold_msg: bool, bold_cpu: bool) -> str:
    if row is None:
        return "—"
    if row.get("status") != "ok":
        return reason_label(row)
    msg = thin_int(row["msg_s"])
    cpu = thin_int(row["cpu_ns_per_msg"])
    if bold_msg:
        msg = f"**{msg}**"
    if bold_cpu:
        cpu = f"**{cpu}**"
    return f"{msg} · {cpu}"


def fmt_lat_cell(row: dict | None, bold_p50: bool, bold_p99: bool) -> str:
    if row is None:
        return "—"
    if row.get("status") != "ok":
        return reason_label(row)
    p50 = thin_int(row["p50_us"])
    p99 = thin_int(row["p99_us"])
    if bold_p50:
        p50 = f"**{p50}**"
    if bold_p99:
        p99 = f"**{p99}**"
    return f"{p50} / {p99}"


def winners_max(values: list[tuple[str, float]]) -> set[str]:
    if not values:
        return set()
    best = max(v for _, v in values)
    return {c for c, v in values if v == best}


def winners_min(values: list[tuple[str, float]]) -> set[str]:
    if not values:
        return set()
    best = min(v for _, v in values)
    return {c for c, v in values if v == best}


def payload_label(payload: int) -> str:
    if payload == 1024:
        return f"1{NBSP}024 B"
    if payload == 4096:
        return f"4{NBSP}096 B"
    return f"{payload} B"


def render_load_table(by: dict, scenario: str, qos: int, ack: str | None) -> str:
    lines = [
        "| Payload | glide | rumqttc | mqtt5 |",
        "|--------:|------:|--------:|------:|",
    ]
    for payload in PAYLOADS:
        clients = by.get((scenario, qos, payload, ack), {})
        ok = [
            (c, clients[c])
            for c in CLIENTS
            if c in clients and clients[c].get("status") == "ok"
        ]
        msg_w = winners_max([(c, r["msg_s"]) for c, r in ok])
        cpu_w = winners_min([(c, r["cpu_ns_per_msg"]) for c, r in ok])
        cells = [
            fmt_load_cell(clients.get(c), c in msg_w, c in cpu_w) for c in CLIENTS
        ]
        lines.append(f"| {payload_label(payload)} | " + " | ".join(cells) + " |")
    return "\n".join(lines)


def render_lat_table(by: dict, scenario: str, qos: int, ack: str | None) -> str:
    lines = [
        "| Payload | glide | rumqttc | mqtt5 |",
        "|--------:|------:|--------:|------:|",
    ]
    for payload in PAYLOADS:
        clients = by.get((scenario, qos, payload, ack), {})
        ok = [
            (c, clients[c])
            for c in CLIENTS
            if c in clients and clients[c].get("status") == "ok"
        ]
        p50_w = winners_min([(c, r["p50_us"]) for c, r in ok])
        p99_w = winners_min([(c, r["p99_us"]) for c, r in ok])
        cells = [
            fmt_lat_cell(clients.get(c), c in p50_w, c in p99_w) for c in CLIENTS
        ]
        lines.append(f"| {payload_label(payload)} | " + " | ".join(cells) + " |")
    return "\n".join(lines)


def section_title(scenario: str) -> str:
    return {
        "ingress_load": "Ingress load",
        "outbound_serial": "Outbound serial",
        "outbound_pipelined": "Outbound pipelined (window 64)",
        "outbound_concurrent": "Outbound concurrent (64 callers)",
        "ingress_serial": "Ingress serial latency",
        "outbound_serial_latency": "Outbound serial latency",
    }[scenario]


def render_broker_block(
    by: dict,
    *,
    load_units: bool,
    omit_outbound_qos0: bool = False,
) -> str:
    """Render load + latency sections for one broker.

    When `omit_outbound_qos0` is set (Mosquitto), skip outbound load/latency
    QoS 0 tables entirely — those cells are wire-unfair without a publish counter.
    """
    parts: list[str] = []
    for scenario in LOAD_SCENARIOS:
        qos_list = (0, 1)
        if omit_outbound_qos0 and scenario in (
            "outbound_serial",
            "outbound_pipelined",
            "outbound_concurrent",
        ):
            qos_list = (1,)
        parts.append(f"### {section_title(scenario)}")
        parts.append("")
        if load_units and scenario == "ingress_load":
            parts.append("Units: msg/s · CPU ns/msg.")
            parts.append("")
        for qos in qos_list:
            if scenario == "ingress_load" and qos == 1:
                for ack in ("after_enqueue", "before"):
                    parts.append(f"#### QoS {qos} · `{ack}`")
                    parts.append("")
                    parts.append(render_load_table(by, scenario, qos, ack))
                    parts.append("")
            else:
                parts.append(f"#### QoS {qos}")
                parts.append("")
                parts.append(render_load_table(by, scenario, qos, None))
                parts.append("")

    for scenario in LATENCY_SCENARIOS:
        qos_list = (0, 1)
        if omit_outbound_qos0 and scenario == "outbound_serial_latency":
            qos_list = (1,)
        parts.append(f"### {section_title(scenario)}")
        parts.append("")
        if scenario == "ingress_serial":
            parts.append("Units: p50 / p99 µs.")
            parts.append("")
        for qos in qos_list:
            if scenario == "ingress_serial" and qos == 1:
                for ack in ("after_enqueue", "before"):
                    parts.append(f"#### QoS {qos} · `{ack}`")
                    parts.append("")
                    parts.append(render_lat_table(by, scenario, qos, ack))
                    parts.append("")
            else:
                parts.append(f"#### QoS {qos}")
                parts.append("")
                parts.append(render_lat_table(by, scenario, qos, None))
                parts.append("")
    return "\n".join(parts).rstrip() + "\n"


def highlight_row(by: dict, scenario: str, qos: int, ack: str | None, kind: str) -> str:
    clients = by.get((scenario, qos, 256, ack), {})
    if kind == "load":
        ok = [
            (c, clients[c])
            for c in CLIENTS
            if c in clients and clients[c].get("status") == "ok"
        ]
        msg_w = winners_max([(c, r["msg_s"]) for c, r in ok])
        cpu_w = winners_min([(c, r["cpu_ns_per_msg"]) for c, r in ok])
        cells = [fmt_load_cell(clients.get(c), c in msg_w, c in cpu_w) for c in CLIENTS]
    else:
        ok = [
            (c, clients[c])
            for c in CLIENTS
            if c in clients and clients[c].get("status") == "ok"
        ]
        p50_w = winners_min([(c, r["p50_us"]) for c, r in ok])
        p99_w = winners_min([(c, r["p99_us"]) for c, r in ok])
        cells = [fmt_lat_cell(clients.get(c), c in p50_w, c in p99_w) for c in CLIENTS]
    return " | ".join(cells)


def render_highlights(by: dict) -> str:
    rows = [
        (
            "Ingress load QoS 1 `after_enqueue`",
            highlight_row(by, "ingress_load", 1, "after_enqueue", "load"),
        ),
        (
            "Ingress load QoS 1 `before`",
            highlight_row(by, "ingress_load", 1, "before", "load"),
        ),
        (
            "Outbound pipelined QoS 1",
            highlight_row(by, "outbound_pipelined", 1, None, "load"),
        ),
        (
            "Outbound concurrent QoS 1",
            highlight_row(by, "outbound_concurrent", 1, None, "load"),
        ),
        (
            "Outbound serial QoS 1",
            highlight_row(by, "outbound_serial", 1, None, "load"),
        ),
        (
            "Ingress serial QoS 1 `after_enqueue` (p50/p99 µs)",
            highlight_row(by, "ingress_serial", 1, "after_enqueue", "lat"),
        ),
        (
            "Outbound serial latency QoS 1 (p50/p99 µs)",
            highlight_row(by, "outbound_serial_latency", 1, None, "lat"),
        ),
    ]
    lines = [
        "| Scenario | glide | rumqttc | mqtt5 |",
        "|----------|------:|--------:|------:|",
    ]
    for label, cells in rows:
        lines.append(f"| {label} | {cells} |")
    return "\n".join(lines)


def render_generated(mosq: dict, mock: dict) -> str:
    return "\n".join(
        [
            MARKER_START,
            "",
            "## Highlights (Mosquitto, 256 B)",
            "",
            "Cutover-relevant where both support the cell. **Bold** = best msg/s or lowest CPU (or lowest p50/p99) among ok clients in that row.",
            "",
            render_highlights(mosq),
            "",
            "---",
            "",
            "## Mosquitto baseline",
            "",
            "Broker: eclipse-mosquitto `2.1.2-alpine` via testcontainers (anonymous; `set_tcp_nodelay`; unlimited inflight/queue; no persistence / `$SYS` / logs).",
            "",
            "**Bold** = best msg/s / lowest CPU ns/msg (load), or lowest p50 / p99 (latency), among ok clients in the row.",
            "",
            "Outbound QoS 0 (load + serial latency) is omitted here — no wire counter without a measuring subscriber; see **Mock ceiling**.",
            "",
            render_broker_block(mosq, load_units=True, omit_outbound_qos0=True),
            "",
            "---",
            "",
            "## Mock ceiling",
            "",
            "Broker: out-of-process mock MQTT 5 (`MQTT_COMPARE_BROKER=mock`) — multi-thread Tokio, Arc fanout, `TCP_NODELAY`, inbound PUBLISH counter, `receive_max` 1024.",
            "Use for client CPU / enqueue ceiling attribution, not as a broker product comparison.",
            "",
            render_broker_block(mock, load_units=False, omit_outbound_qos0=False),
            "",
            "---",
            "",
            "## Refresh",
            "",
            "```shell",
            "# Mosquitto baseline (Podman or Docker)",
            "MQTT_COMPARE_QOS=0,1 \\",
            "MQTT_COMPARE_PAYLOADS=64,256,1024,4096 \\",
            "MQTT_COMPARE_RUNS=100 \\",
            "MQTT_COMPARE_OUT=target/mqtt_compare/full-mosquitto-100 \\",
            "  cargo bench --bench mqtt_compare",
            "",
            "# Mock ceiling",
            "MQTT_COMPARE_BROKER=mock \\",
            "MQTT_COMPARE_QOS=0,1 \\",
            "MQTT_COMPARE_PAYLOADS=64,256,1024,4096 \\",
            "MQTT_COMPARE_RUNS=100 \\",
            "MQTT_COMPARE_OUT=target/mqtt_compare/full-mock-100 \\",
            "  cargo bench --bench mqtt_compare",
            "",
            "# Rebuild tables (bold winners) from JSONL",
            "python3 benches/render_benchmarks.py",
            "```",
            "",
            "Then sync the README highlight table if the Mosquitto 256 B story changes.",
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
        new_text = pattern.sub(generated.strip(), text, count=1)
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


def ensure_reading_note(path: Path) -> None:
    text = path.read_text()
    needle = "- Load cells: **msg/s · CPU ns/msg**"
    note = (
        "- **Bold** in a row: best msg/s and lowest CPU (load), or lowest p50 and p99 (latency), "
        "among clients with `status: ok` (ties all bold)."
    )
    if "Bold** in a row" in text:
        return
    if needle not in text:
        return
    path.write_text(text.replace(needle, note + "\n" + needle, 1))


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mosquitto",
        type=Path,
        default=root / "target/mqtt_compare/full-mosquitto-100",
    )
    parser.add_argument(
        "--mock",
        type=Path,
        default=root / "target/mqtt_compare/full-mock-100",
    )
    parser.add_argument("--out", type=Path, default=root / "BENCHMARKS.md")
    args = parser.parse_args()

    mosq = index_rows(load_rows(args.mosquitto))
    mock = index_rows(load_rows(args.mock))
    generated = render_generated(mosq, mock)
    patch_file(args.out, generated)
    ensure_reading_note(args.out)
    print(f"wrote tables into {args.out}")


if __name__ == "__main__":
    main()
