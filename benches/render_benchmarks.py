#!/usr/bin/env python3
"""Render the generated parts of BENCHMARKS.md and the detail pages in benches/results/.

Input is one results directory (default `target/bench-results`) with one subdirectory per
bench run, named as in the refresh script in BENCHMARKS.md:

  peers-mock, peers-mosquitto, peers-mosquitto-stock   mqtt_compare, all clients
  capped-mock                                          mqtt_compare, mock capping deliveries at 32
  payload                                              mqtt_compare, payload sizes (mock)
  fanin                                                ingress_fanin

BENCHMARKS.md and benches/results/inbound-sinks.md keep their prose; only blocks between `<!-- bench:NAME:start -->` and
`<!-- bench:NAME:end -->` are replaced. Missing runs render as "not measured".

Usage:
  python3 benches/render_benchmarks.py [--results DIR] [--out BENCHMARKS.md] [--detail benches/results]
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
from collections import defaultdict
from pathlib import Path

CLIENTS = ("glide", "rumqttc", "mqtt5")
PAYLOAD = 256
NBSP = "\u202f"
PAYLOADS = (16, 256, 1024, 16384)

SCENARIOS = (
    ("outbound_concurrent", "Outbound concurrent (callers sharing one client)"),
    ("outbound_pipelined", "Outbound pipelined (window 64, one task)"),
    ("outbound_rate", "Outbound rate (open loop, offered msg/s)"),
    ("ingress_load", "Ingress load (publisher bursts at the client)"),
    ("ingress_rate", "Ingress rate (open loop, offered msg/s)"),
)
OUTBOUND = {s for s, _ in SCENARIOS if s.startswith("outbound")}
LEVEL_LABEL = {
    "outbound_concurrent": lambda lv: f"{lv} callers",
    "outbound_rate": lambda lv: f"{fmt_int(lv)} msg/s offered",
    "ingress_rate": lambda lv: f"{fmt_int(lv)} msg/s offered",
}
SINKS = ("mpsc", "flume", "async_channel", "shard", "inline")


# ---------------------------------------------------------------- loading


def load_run(path: Path) -> tuple[dict, dict] | None:
    """(index, meta) of an mqtt_compare run dir, or None if absent."""
    if not path.is_dir():
        return None
    index: dict = defaultdict(dict)
    for jsonl in sorted(path.glob("*.jsonl")):
        for line in jsonl.read_text().splitlines():
            if line.strip():
                row = json.loads(line)
                key = (row["scenario"], row["qos"], row["payload"], row.get("level"), row.get("ack"))
                index[key][row["client"]] = row
    meta_path = path / "meta.json"
    meta = json.loads(meta_path.read_text()) if meta_path.exists() else {}
    return index, meta


def load_fanin(path: Path) -> list[dict] | None:
    file = path / "fanin.jsonl"
    if not file.exists():
        return None
    return [json.loads(line) for line in file.read_text().splitlines() if line.strip()]


# ---------------------------------------------------------------- formatting


def fmt_int(value: float) -> str:
    return f"{value:,.0f}".replace(",", NBSP)


def fmt_rate(value: float) -> str:
    """Compact msg/s: 2.71M, 364k, 9 976."""
    if value >= 1e6:
        return f"{value / 1e6:.2f}M"
    if value >= 1e5:
        return f"{value / 1e3:.0f}k"
    return fmt_int(value)


def fmt_us(value: float) -> str:
    return f"{value:,.1f}".replace(",", NBSP) if value < 100 else fmt_int(value)


def spread(runs: list[float] | None) -> str:
    """Half the min-max range of the runs relative to their median, e.g. `±3%`."""
    if not runs or len(runs) < 2:
        return ""
    mid = statistics.median(runs)
    if mid <= 0:
        return ""
    pct = (max(runs) - min(runs)) / 2 / mid * 100
    return f" ±{pct:.0f}%" if pct >= 0.5 else " ±0%"


def bold_if(text: str, win: bool) -> str:
    return f"**{text}**" if win else text


def na(row: dict | None) -> str | None:
    if row is None:
        return "—"
    if row.get("status") != "ok":
        return f"n/a (`{row.get('reason') or 'unsupported'}`)"
    return None


def winners(ok: dict[str, dict], key: str, best) -> set[str]:
    vals = [(c, r[key]) for c, r in ok.items() if r.get(key) is not None]
    if not vals:
        return set()
    target = best(v for _, v in vals)
    return {c for c, v in vals if v == target}


def load_cell(row: dict | None, ok: dict[str, dict], client: str, *, latency: bool = True) -> str:
    """`msg/s ±spread · CPU ns/msg [· p50 / p99 / p99.9 µs]`, best per metric bold."""
    missing = na(row)
    if missing:
        return missing
    if "msg_s" not in row:
        keys = ("p50_us", "p99_us")
        return " / ".join(
            bold_if(fmt_us(row[k]), client in winners(ok, k, min)) for k in keys if k in row
        ) + " µs"
    parts = [
        bold_if(fmt_int(row["msg_s"]), client in winners(ok, "msg_s", max)) + spread(row.get("msg_s_runs")),
        bold_if(fmt_int(row["cpu_ns_per_msg"]), client in winners(ok, "cpu_ns_per_msg", min)),
    ]
    if latency and row.get("p50_us") is not None:
        keys = [k for k in ("p50_us", "p99_us", "p999_us") if row.get(k) is not None]
        parts.append(" / ".join(bold_if(fmt_us(row[k]), client in winners(ok, k, min)) for k in keys) + " µs")
    return " · ".join(parts)


def ok_rows(clients: dict) -> dict[str, dict]:
    return {c: r for c, r in clients.items() if r.get("status") == "ok"}


def client_row(clients: dict, **kw) -> str:
    ok = ok_rows(clients)
    return " | ".join(load_cell(clients.get(c), ok, c, **kw) for c in CLIENTS)


def header(first: str) -> list[str]:
    return [f"| {first} | glide | rumqttc | mqtt5 |", "|---|---:|---:|---:|"]


NOT_MEASURED = "_Not measured in this refresh._\n"
ACK_NOTE = (
    "Ingress QoS 1 has a row per ack policy: `after_enqueue` sends PUBACK once the message is in the app queue, "
    "`before` on decode. Each client runs the policies it has (glide only `after_enqueue`, mqtt5 only `before`); "
    "`before` saves the client work and can lose a message the app never saw."
)
BOLD_NOTE = (
    "**Bold** = best value in the row (highest msg/s, lowest CPU ns/msg and latency) among clients that ran; "
    "`±` = half the min–max range of the runs relative to the median."
)


# ---------------------------------------------------------------- peer detail pages


def detail_table(by: dict, scenario: str, qos: int, ack: str | None) -> str:
    keys = [k for k in by if k[0] == scenario and k[1] == qos and k[2] == PAYLOAD and k[4] == ack]
    levels = sorted({k[3] for k in keys}, key=lambda x: (x is None, x))
    if not levels:
        return ""
    first = "Level" if levels != [None] else "Payload"
    lines = header(first)
    for lv in levels:
        clients = by.get((scenario, qos, PAYLOAD, lv, ack), {})
        label = LEVEL_LABEL[scenario](lv) if lv is not None else f"{PAYLOAD} B"
        lines.append(f"| {label} | {client_row(clients)} |")
    return "\n".join(lines)


def units(scenario: str) -> str:
    if scenario in ("outbound_concurrent", "outbound_rate"):
        return "Units: msg/s · CPU ns/msg · p50 / p99 / p99.9 µs per publish."
    if scenario == "ingress_rate":
        return "Units: msg/s · CPU ns/msg · p50 / p99 / p99.9 µs from the publisher's write to the consumer taking the message off the app queue."
    return "Units: msg/s · CPU ns/msg."


def render_detail(title: str, run: tuple[dict, dict] | None, intro: str) -> str:
    lines = [f"# {title}", "", "Generated by `benches/render_benchmarks.py`; do not edit by hand.", ""]
    if run is None:
        return "\n".join(lines + [NOT_MEASURED])
    by, meta = run
    lines += [intro, "", meta_line(meta), "", BOLD_NOTE, ""]
    for scenario, stitle in SCENARIOS:
        sec: list[str] = []
        for qos in (0, 1):
            acks = ("after_enqueue", "before") if scenario.startswith("ingress") and qos == 1 else (None,)
            for ack in acks:
                body = detail_table(by, scenario, qos, ack)
                if body:
                    tag = f" · `{ack}`" if ack else ""
                    sec += [f"### QoS {qos}{tag}", "", body, ""]
        if sec:
            lines += [f"## {stitle}", "", units(scenario), ""] + sec
    return "\n".join(lines).rstrip() + "\n"


def meta_line(meta: dict) -> str:
    if not meta:
        return ""
    bits = [
        f"Broker: {meta.get('broker', '?')}",
        f"runs: median of {meta.get('runs', '?')}",
        f"burst: {fmt_int(meta.get('load_messages', 0))} messages",
        f"client Receive Maximum {meta.get('client_receive_max', '?')}",
        f"Tokio workers {meta.get('worker_threads') or 'default'}",
        f"CPUs client {meta.get('client_cpus', '?')} / broker {meta.get('broker_cpus', '?')} / publisher {meta.get('publisher_cpus', '?')}",
    ]
    return "; ".join(bits) + "."


# ---------------------------------------------------------------- BENCHMARKS.md blocks


ACKS = (("after_enqueue", "PUBACK after the app queue"), ("before", "PUBACK on decode"))


def ingress_rows(by: dict, label: str, scenario: str, qos: int, level=None, payload: int = PAYLOAD, **kw) -> list[str]:
    """One row for QoS 0, one per ack policy for QoS 1."""
    if qos == 0:
        return [f"| {label}, QoS 0 | {client_row(by.get((scenario, 0, payload, level, None), {}), **kw)} |"]
    return [
        f"| {label}, QoS 1, {text} | {client_row(by.get((scenario, 1, payload, level, ack), {}), **kw)} |"
        for ack, text in ACKS
    ]


def block_summary(stock, mock) -> str:
    out = []
    if stock is None:
        out.append(NOT_MEASURED)
    else:
        by = stock[0]
        rows = (
            ("Outbound concurrent, 8 callers", ("outbound_concurrent", 1, PAYLOAD, 8, None)),
            ("Outbound concurrent, 64 callers", ("outbound_concurrent", 1, PAYLOAD, 64, None)),
            ("Outbound concurrent, 512 callers", ("outbound_concurrent", 1, PAYLOAD, 512, None)),
            ("Outbound pipelined", ("outbound_pipelined", 1, PAYLOAD, None, None)),
            ("Outbound rate, 50 000 msg/s offered", ("outbound_rate", 1, PAYLOAD, 50000, None)),
        )
        out += ["**Real broker**: stock Mosquitto (its default flow control), QoS 1 unless noted, 256 B.", ""] + header("Scenario")
        for label, key in rows:
            out.append(f"| {label} | {client_row(by.get(key, {}))} |")
        for qos in (0, 1):
            out += ingress_rows(by, "Ingress load", "ingress_load", qos, latency=False)
        out += ingress_rows(by, "Ingress rate 50 000 msg/s", "ingress_rate", 1, level=50000)
        out.append("")
    if mock is not None:
        by = mock[0]
        out += ["**Client ceiling**: mock broker, 256 B.", ""] + header("Scenario")
        for qos in (0, 1):
            out.append(
                f"| Outbound concurrent 512, QoS {qos} | {client_row(by.get(('outbound_concurrent', qos, PAYLOAD, 512, None), {}))} |"
            )
        for qos in (0, 1):
            out.append(
                f"| Outbound pipelined, QoS {qos} | {client_row(by.get(('outbound_pipelined', qos, PAYLOAD, None, None), {}))} |"
            )
        for qos in (0, 1):
            out += ingress_rows(by, "Ingress load", "ingress_load", qos, latency=False)
        out.append("")
    out += ["Units: msg/s ±spread · CPU ns/msg · p50 / p99 / p99.9 µs where the scenario has latency.", BOLD_NOTE, ACK_NOTE]
    return "\n".join(out) + "\n"


def block_capped(capped) -> str:
    if capped is None:
        return NOT_MEASURED
    by = capped[0]
    out = header("Scenario")
    out += ingress_rows(by, "Ingress load", "ingress_load", 1, latency=False)
    for level in (10000, 50000, 200000):
        out += ingress_rows(by, f"Ingress rate {fmt_int(level)} msg/s", "ingress_rate", 1, level=level)
    return "\n".join(out + ["", "Units: msg/s ±spread · CPU ns/msg · p50 / p99 / p99.9 µs. " + ACK_NOTE]) + "\n"


def block_tuned(tuned) -> str:
    if tuned is None:
        return NOT_MEASURED
    by = tuned[0]
    out = header("Scenario")
    for label, key in (
        ("Outbound concurrent, 64 callers", ("outbound_concurrent", 1, PAYLOAD, 64, None)),
        ("Outbound concurrent, 512 callers", ("outbound_concurrent", 1, PAYLOAD, 512, None)),
        ("Outbound pipelined", ("outbound_pipelined", 1, PAYLOAD, None, None)),
    ):
        out.append(f"| {label} | {client_row(by.get(key, {}))} |")
    out += ingress_rows(by, "Ingress load", "ingress_load", 1, latency=False)
    return "\n".join(out + ["", "Units: msg/s ±spread · CPU ns/msg · p50 / p99 / p99.9 µs per publish."]) + "\n"


def block_payload(payload, mock) -> str:
    if payload is None:
        return NOT_MEASURED
    merged: dict = defaultdict(dict)
    for src in (payload, mock):
        if src is None:
            continue
        for key, clients in src[0].items():
            merged[key].update(clients)
    out = []
    for title, scenario, qos in (
        ("Ingress load, QoS 0", "ingress_load", 0),
        ("Ingress load, QoS 1, PUBACK after the app queue (`after_enqueue`)", "ingress_load", 1),
        ("Outbound pipelined, QoS 0", "outbound_pipelined", 0),
        ("Outbound pipelined, QoS 1", "outbound_pipelined", 1),
    ):
        out += [f"**{title}**:", ""] + header("Payload")
        ack = "after_enqueue" if scenario == "ingress_load" and qos == 1 else None
        for size in PAYLOADS:
            clients = merged.get((scenario, qos, size, None, ack))
            if clients:
                mbs = {c: r["msg_s"] * size / 1e6 for c, r in clients.items() if r.get("status") == "ok"}
                cells = []
                ok = ok_rows(clients)
                for c in CLIENTS:
                    text = load_cell(clients.get(c), ok, c, latency=False)
                    if c in mbs:
                        text += f" · {mbs[c]:,.0f} MB/s".replace(",", NBSP)
                    cells.append(text)
                out.append(f"| {fmt_int(size)} B | {' | '.join(cells)} |")
        out.append("")
    out.append("Units: msg/s ±spread · CPU ns/msg · payload MB/s. 256 B comes from the peer run.")
    return "\n".join(out) + "\n"


def block_fanin(rows: list[dict] | None) -> str:
    if not rows:
        return NOT_MEASURED
    out = []
    by = {(r["qos"], r["work_ns"], r["brokers"], r["consumers"], r["sink"]): r for r in rows}
    for qos in sorted({r["qos"] for r in rows}):
        for work in sorted({r["work_ns"] for r in rows}):
            sub = [r for r in rows if r["qos"] == qos and r["work_ns"] == work]
            if not sub:
                continue
            out += [f"**QoS {qos}, {work} ns work per message:**", ""]
            out += ["| Brokers × consumers | " + " | ".join(f"`{s}`" for s in SINKS) + " |", "|---|" + "---:|" * len(SINKS)]
            pairs = sorted({(r["brokers"], r["consumers"]) for r in sub if r["sink"] != "inline"} | {(r["brokers"], r["consumers"]) for r in sub if r["sink"] == "inline"})
            for n, m in pairs:
                cells = {s: by.get((qos, work, n, m, s)) for s in SINKS}
                present = {s: r for s, r in cells.items() if r}
                best = max((r["msg_s"] for r in present.values()), default=None)
                texts = []
                for s in SINKS:
                    r = cells[s]
                    if not r:
                        texts.append("")
                        continue
                    t = bold_if(fmt_rate(r["msg_s"]), r["msg_s"] == best) + spread(r.get("msg_s_runs"))
                    t += f" · {fmt_int(r['cpu_ns_per_msg'])}"
                    if r.get("stalls_per_kmsg", 0) >= 0.05:
                        t += f" · {r['stalls_per_kmsg']:.1f}"
                    texts.append(t)
                out.append(f"| {n} × {m} | " + " | ".join(texts) + " |")
            out.append("")
    out.append(
        "Cells: msg/s ±spread · CPU ns/msg · handoff stalls per 1000 messages (omitted when ~0). "
        "**Bold** = fastest sink in the row. `mpsc` only runs with one consumer; `inline` runs the work on each "
        "connection's read task, so its consumer count is the broker count."
    )
    return "\n".join(out) + "\n"


def patch_blocks(path: Path, blocks: dict[str, str]) -> None:
    text = path.read_text()
    for name, body in blocks.items():
        start, end = f"<!-- bench:{name}:start -->", f"<!-- bench:{name}:end -->"
        if start not in text or end not in text:
            raise SystemExit(f"{path}: missing block markers for {name!r}")
        pattern = re.compile(re.escape(start) + r".*?" + re.escape(end), re.DOTALL)
        text = pattern.sub(lambda _: f"{start}\n\n{body.strip()}\n\n{end}", text, count=1)
    path.write_text(text)


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--results", type=Path, default=root / "target/bench-results")
    parser.add_argument("--out", type=Path, default=root / "BENCHMARKS.md")
    parser.add_argument("--detail", type=Path, default=root / "benches/results")
    args = parser.parse_args()
    r = args.results

    mock = load_run(r / "peers-mock")
    mosq = load_run(r / "peers-mosquitto")
    stock = load_run(r / "peers-mosquitto-stock")

    patch_blocks(
        args.out,
        {
            "summary": block_summary(stock, mock),
            "tuned": block_tuned(mosq),
            "capped": block_capped(load_run(r / "capped-mock")),
            "payload": block_payload(load_run(r / "payload"), mock),
        },
    )
    args.detail.mkdir(parents=True, exist_ok=True)
    patch_blocks(args.detail / "inbound-sinks.md", {"fanin": block_fanin(load_fanin(r / "fanin"))})
    pages = (
        ("mosquitto-stock.md", "Peer comparison: stock Mosquitto (Receive Maximum 20)", stock,
         "eclipse-mosquitto 2.1.2-alpine with its default flow control: `max_inflight_messages 20` (CONNACK Receive Maximum 20), `max_queued_messages 1000`; `set_tcp_nodelay`, no persistence. Outbound QoS 0 is not run: without a wire counter the end of a burst is unknown."),
        ("mosquitto.md", "Peer comparison: Mosquitto without flow control (tuned)", mosq,
         "Same image with the flow control switched off: `max_inflight_messages 0` (CONNACK Receive Maximum 65 535), `max_queued_messages 0`. Outbound QoS 0 is not run: without a wire counter the end of a burst is unknown."),
        ("mock.md", "Peer comparison: mock broker (client ceiling)", mock,
         "Out-of-process mock MQTT 5 broker: multi-thread, honours the client's Receive Maximum, handles every buffered frame before it writes, counts decoded PUBLISH frames so outbound QoS 0 has a wire-complete end. It is faster than every client, so these tables show what each client can do."),
    )
    for name, title, run, intro in pages:
        (args.detail / name).write_text(render_detail(title, run, intro))
    print(f"wrote {args.out} and {args.detail}/")


if __name__ == "__main__":
    main()
