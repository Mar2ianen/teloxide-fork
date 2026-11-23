# Teloxide Bot API 10.5 quality testing

## Regular checks

Every PR compiles all targets, checks formatting and Clippy, and runs
core and high-level tests across stable, beta, nightly and the workspace MSRV.
The offline fake Bot API runs without a Telegram token or network. It covers
`getMe`, channel posts from `Channel_Bot`, and error responses.

The tests in `bot_api_10_5_properties.rs` use `proptest` to verify
tri-state optional settings and independent self-capability flags. Proptest's
regression files should be committed when a new shrinking counterexample is
found, so future runs reproduce the exact issue.

## Generator

The code generator is intentionally **check-only by default**: stale files
fail with a message and do not modify the source tree. To regenerate:

```sh
TELOXIDE_CODEGEN_WRITE=1 cargo test -p teloxide-core --features full codegen_
cargo test -p teloxide-core --features full codegen_
git diff --check
```

`RUSTFMT` can be set to the path of a rustfmt binary; no hardcoded
`rustup run nightly-2025-06-12` is required by the generator itself.
The repo still pins nightly for **byte-for-byte reproducible historical
formatting**, because `rustfmt.toml` uses nightly-only options. Stable
rustfmt can run with reduced formatting features but may generate a diff.
The user-facing compile path remains stable, including the 1.85 MSRV.

## Deep quality

`.github/workflows/deep-testing.yml` runs on pull requests, pushes to `next`,
weekly, and on explicit dispatch. Mutation tests are reserved for non-PR runs:

- `cargo-nextest` / `cargo-hack`: focused test suite and feature matrix;
- `cargo-llvm-cov`: line coverage artifact (not a quality target by itself);
- `cargo-mutants`: mutants in the typed self-check implementation;
- `cargo-fuzz`: short libFuzzer sessions on incoming updates and rich
  identity payloads. Long fuzz campaigns can run locally using the same
  targets. Avoid real bot tokens and use only sanitized fixtures.

Run locally:

```sh
cargo +stable nextest run -p teloxide-core --features full
cargo +stable hack check -p teloxide-core --each-feature --exclude-features nightly --no-dev-deps
cargo +stable llvm-cov -p teloxide-core --features full --lcov --output-path coverage.lcov
cargo +stable mutants -p teloxide-core --file crates/teloxide-core/src/self_check.rs -- --features full
cargo +nightly fuzz run updates -- -max_total_time=60
```

## Runtime capability checks

`teloxide_core::self_check::{inspect_bot, check_bot}` compare a `getMe`
snapshot against application-declared expectations. They never invoke
`setMySettings`. A disabled feature is not an error unless an application
expects it to be enabled.

`CacheMe` automatically invalidates the shared `getMe` cache for all clones
after successful `setMySettings` and `setMyName` requests made **through the
adaptor**. Failed, unsent and canceled requests do not invalidate it. In-flight
`getMe` results belong to their original generation, so they cannot repopulate
the new cache after invalidation. Explicit `clear()` also invalidates all clones.

If the bot was reconfigured outside this adaptor, or a strongly fresh snapshot
is required, pass `cache.inner()` to `check_bot` to bypass the cache.
Unsupported capabilities must not be inferred from `BotFullInfo`.

## Live Telegram

The locally mocked HTTP API is **not** an authoritative emulator of Telegram
server semantics. Verify behavioral compatibility against the official
Telegram test environment in a separately configured, secret-protected CI
job or before releasing; do not give tokens to untrusted pull requests.

## Downstream compatibility

The advisory `.github/workflows/downstream-compat.yml` checks public
`NedoBot` and `teloxide-ui` against the current Teloxide checkout, overriding
only their pinned Git dependency in **disposable worktrees**. The source
projects are never modified. Targets are documented in `downstream.toml` and
can be checked locally with:

```sh
python3 scripts/check_downstream.py teloxide-ui
python3 scripts/check_downstream.py nedobot
```

These compatibility jobs are advisory until consumers migrate: an unrelated
consumer build failure must not silently block a library release. The Telegram
`Channel_Bot` and `GroupAnonymousBot` are internal synthetic identities,
not downstream repositories; they are covered by offline fixtures.
