# Contributing

Thanks for contributing to **mqtt-glide**.

## Development

```shell
cargo test --lib
cargo clippy -- -D warnings
cargo fmt --check
```

TLS features are mutually exclusive. To verify both providers:

```shell
cargo test --lib
cargo test --no-default-features --features tls-rustls-ring --lib
```

Broker integration tests need Docker or Podman:

```shell
cargo test --test brokers
cargo test --test connection
```

Public API and product scope: [`README.md`](README.md). MUST tracker: [`tests/conformance/checklist.md`](tests/conformance/checklist.md).

## Commit messages

We follow [Conventional Commits](https://www.conventionalcommits.org/).

### Format

```
<type>(<scope>): <description>

[optional body]
```

### Types

| Type       | When to use                                          |
|------------|------------------------------------------------------|
| `feat`     | New feature                                          |
| `fix`      | Bug fix                                              |
| `chore`    | Maintenance, tooling, no logic change                |
| `refactor` | Restructuring without behaviour change               |
| `perf`     | Performance improvement                              |
| `test`     | Adding or changing tests                             |
| `docs`     | Documentation only                                   |
| `style`    | Formatting, no logic impact                          |
| `ci`       | CI/CD pipeline changes                               |

### Rules

- **Imperative mood** in the description: "add", "fix", "remove" — not "added", "fixes"
- **Lowercase** after the colon
- **No period** at the end of the description
- **Max 72 characters** on the first line
- **Scope is optional** but useful: `feat(sink): …`, `fix(keepalive): …`

### Body

Use a body when the **why** is not obvious from the diff.
Two to four sentences is the upper limit. No bullet lists that only mirror the file list.

```
fix(keepalive): skip ping watchdog during handoff stall

A full InboundSink must not look like a dead peer; the write task
already keeps PINGREQ alive while read waits on backpressure.
```

### Examples

```
feat(builder): add wait_connected option
fix(write): flush inbound pubacks before disconnect
chore: bump MSRV note for rumqttc bench
perf(codec): avoid extra copy in encode_publish
test: cover shared filter no-local rejection
docs: clarify Cap vs Receive Maximum
```

## Pull requests

- Keep changes focused; match existing module style
- Update `README.md` / the conformance checklist when scope or MUST coverage changes
- Do not enable both TLS features or use `--all-features`
