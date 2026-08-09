# AGENTS.md

This file defines the working rules for coding agents in this repository.
It applies to the entire repository unless a more specific `AGENTS.md` exists in a subdirectory.

## Repository purpose

This repository is a development fork of `teloxide` focused on keeping Telegram Bot API support current while preserving upstream compatibility and code quality.

## Branch, release, and versioning policy

`master` is the supported **release line**, not the day-to-day integration
branch. A code commit on `master` is eligible for release only when it belongs
to a versioned release or an explicitly versioned hotfix. An administrative
governance commit is not a crate release. Do not use `master` as a scratch
branch and do not merge ordinary feature work there.

- `next` is the normal integration branch for fork work. Feature branches are
  based on `next` and their PRs target `next`.
- `master` receives only release PRs, explicitly authorized hotfix PRs, or
  the governance exception described below. A release PR promotes a reviewed
  range from `next` to `master`; a hotfix branch starts from the exact commit
  pointed to by its base package tag. A release-blocking fix is not a separate
  route: include it in the existing release/hotfix branch and PR, or develop it
  on `next` for the next release.
- An explicitly requested repository-governance PR may update `AGENTS.md`, CI
  policy, or release documentation independently. It is an administrative
  change, not a crate release, and must not receive a package release tag.
  After it merges into `master`, the same governance state must be reconciled
  into `next` before ordinary feature work resumes.
- Upstream synchronization and broad/high-risk work happen on `next`. Do not
  mix an upstream sync into a release PR unless the release description lists
  and validates it explicitly.
- A release PR must state its target versions, Bot API version, changelog
  entry, package impact, and exact tag plan. It is not ready to merge merely
  because the code is green.

### Release promotion workflow

1. Develop and integrate on `next` through focused feature PRs.
2. Choose a release branch name that does not pretend independently versioned
   crates share one number: use `release/<release-id>` for a multi-package
   release train, `release/<package>-vX.Y.Z` for a single-package release, or
   `hotfix/<package>-vX.Y.Z` for a package-specific hotfix. Start release
   branches from the reviewed `next` head; start hotfix branches from the exact
   commit pointed to by the base package tag, never from an untagged `master`
   head, and record that tag in the PR body. Package keys are `teloxide`,
   `core`, and `macros`, matching the published tag prefixes.
3. Update the semver versions of every affected published crate in its own
   `Cargo.toml`, update `CHANGELOG.md`, and document the release scope. Keep
   `teloxide`, `teloxide-core`, and `teloxide-macros` versioned independently;
   do not invent a workspace version that is not the source of truth.
4. Run the complete release validation matrix and review the package diffs,
   generated output, lockfile effects, and public API changes.
5. Merge the release PR into `master`. The merge commit is the release source;
   do not continue ordinary development on it.
6. Create immutable annotated tags on that exact merge commit for each package
   being released: `vX.Y.Z` for `teloxide`, `core-vX.Y.Z` for
   `teloxide-core`, and `macros-vX.Y.Z` for `teloxide-macros`. Never move or
   reuse a published tag.
7. Publish crates only from the tagged release commit. After tagging,
   reconcile `next` with that release commit before starting the next work
   cycle, then continue development with the next unreleased version plan.
   `HEAD` of `master` without a release tag is not considered released.

### Governance propagation

A governance PR can be untagged on `master`, but it cannot remain isolated
there while `next` is used for ordinary development:

1. Record the merged governance commit SHA on `master`.
2. Create an `integration/governance-<short-id>` branch from `next` and apply
   the same governance commit/diff. Open and merge a PR into `next`; do not
   silently discard unrelated `next` work or use a full `master` merge as a
   substitute unless a release synchronization is intended.
3. Block ordinary feature branches and feature PRs from `next` until `next`
   contains the current governance state from `master`. The integration PR
   carries the source `master` SHA in its description for traceability.

The pre-feature check is therefore two-part: `next` must contain the latest
applicable tagged `master` release **and** a reconciled governance state
corresponding to every governance commit that landed on `master` after that
release tag. The integration PR must record each source `master` SHA; the gate
checks state plus traceability, not ancestry of the original governance commit.
If either condition fails, reconcile first.

### Versioning rules

- Follow semver independently per published crate, with the stability level
  taken into account. For crates at `1.0.0` or later, use patch for compatible
  fixes/refactors, minor for compatible public additions, and major for
  breaking public API changes. For current `0.y.z` crates, use patch for
  compatible fixes/refactors and minor for public additions or breaking public
  changes; do not jump to `1.0.0` for an ordinary pre-1.0 breaking change.
  `1.0.0` is a deliberate project-wide stability milestone. A dependency or
  generated API change may require bumps in more than one crate; explain the
  dependency graph in the release PR.
- A change can be developed on `next` without changing the published version.
  Before it reaches `master`, it must be included in a release PR with the
  appropriate version bump. An urgent release-blocking fix uses the same
  package-specific hotfix PR and patch bump; it is not an independent route
  for changing `master`.
- Crate semver and Telegram Bot API version are separate axes. Record both in
  the release PR and changelog; never imply that a Bot API update automatically
  determines a Rust crate version.
- Release tags are the only deployment/release authority. Do not infer the
  deployed version from branch names, `HEAD`, or a successful CI run.

## Start every task by checking the repository state

Before editing code:

```shell
git status --short --branch
git fetch --all --prune
git branch --show-current
```

Choose the base explicitly instead of switching to `master` automatically:

```shell
# Normal feature work: integrate through next.
git switch next
git pull --ff-only origin next
git switch -c <type>/<short-task-name>

# Release preparation: start from the reviewed next head.
git switch next
git pull --ff-only origin next
git switch -c release/<release-id>       # release train
# or: git switch -c release/<package>-vX.Y.Z

# Hotfix preparation: use the exact base package tag, never current master HEAD.
git fetch --tags origin
git switch --detach <base-package-tag>
git switch -c hotfix/<package>-vX.Y.Z
```

Only release/hotfix or explicitly authorized governance work should switch
to `master`. Do not create ordinary feature branches from `master` or target
ordinary feature PRs there. Before branching from `next`, verify both that it
contains the latest applicable tagged `master` release and that it contains a
reconciled governance state corresponding to every governance commit from
`master` after that tag. Verify the source `master` SHAs in the integration PR;
check state and traceability, not ancestry of the original commits. If either
condition fails, reconcile through an explicit integration PR before starting
new feature work.

Do not assume that a SHA, API version, generated file, package version, release
tag, or known TODO from an old conversation is still current. Inspect the
branch, latest release tags, package manifests, and relevant source files first.

Useful initial commands:

```shell
git log --oneline --decorate -20
just --list
cargo metadata --no-deps --format-version 1 >/dev/null
```

## Non-negotiable rules

1. Do not push directly to `master`; update it through a reviewed, versioned
   release/hotfix PR, or through the explicitly authorized governance exception.
   Tag the exact resulting merge commit only for release/hotfix code changes;
   governance merges receive no package tag.
2. Do not use stale technical branches as a base
3. Do not edit generated files as the primary source of a change
4. Do not claim complete Telegram Bot API coverage without an external audit against the official documentation
5. Do not treat green codegen as proof that the local schemas match Telegram
6. Do not add `InputFile`-containing fields without tracing multipart transport end to end
7. Do not silently drop unsupported rich entities or media
8. Do not weaken tests, lints, derives, or public types merely to make code compile
9. Do not add broad `allow` attributes without a narrow, documented reason
10. Do not merge code that has not passed the relevant test matrix

## Source-of-truth hierarchy

For Telegram Bot API work, use sources in this order:

1. Official Telegram Bot API documentation and changelog
2. An archived documentation snapshot for the exact target API version
3. `crates/teloxide-core/schema.ron` for method code generation
4. `crates/teloxide-core/custom_v2.json` for schema consistency checks
5. Generated Rust files

`schema.ron` and `custom_v2.json` are hand-maintained mirrors. They can agree with each other and still both be wrong.

When updating the API, audit all of the following:

- new methods
- removed or renamed methods
- new parameters on existing methods
- required versus optional parameters
- parameter types and numeric ranges
- return types
- new objects and fields
- renamed or replaced fields
- enum variants and tagged-union representation
- multipart behavior
- serialization names
- documentation-only semantic changes

## Repository map

Important locations:

```text
crates/teloxide-core/schema.ron
    Handwritten method schema used by codegen

crates/teloxide-core/custom_v2.json
    Independent method/object schema used by consistency tests

crates/teloxide-core/src/codegen/
    Schema parsing, patching, checks, and generators

crates/teloxide-core/src/payloads/
    Generated request payloads and payload codegen

crates/teloxide-core/src/requests/
    Requester traits, request types, and generated adaptor fan-out

crates/teloxide-core/src/types/
    Telegram API objects, enums, IDs, serde implementations, and media traversal

crates/teloxide-core/src/serde_multipart/
    Multipart form construction and wire-format tests

crates/teloxide/src/utils/render/
    HTML and MarkdownV2 rendering

.github/workflows/ci.yml
    Authoritative GitHub CI matrix

Justfile
    Fast local development commands

CODE_STYLE.md
    Project Rust and documentation style

CONTRIBUTING.md
    Upstream contribution and Bot API update guidance
```

## Generated code workflow

Generated files normally contain a preamble similar to:

```rust
//! Generated by `codegen_payloads`, do not edit by hand.
```

For method changes, edit both schemas first:

```text
crates/teloxide-core/schema.ron
crates/teloxide-core/custom_v2.json
```

Then run the codegen checks:

```shell
cargo test -p teloxide-core --features "full nightly" codegen -- --nocapture
```

Some generators update files and make the first run fail to show a diff. Inspect the failure rather than blindly ignoring it, then rerun until the command exits successfully with a clean working tree apart from intended changes.

After codegen:

```shell
cargo fmt --all
git diff --check
git status --short
```

Inspect generated changes. A generator can faithfully generate incorrect code from an incorrect schema.

Never hand-edit generated payload, requester, or adaptor output without also changing the generator or its source schema. A temporary diagnostic edit must not survive into the final commit.

## Telegram Bot API update procedure

For each target API version:

1. Freeze the exact official changelog and documentation snapshot
2. Build a checklist of methods, method parameters, objects, fields, variants, and semantic changes
3. Add or update dependent types first
4. Update `schema.ron`
5. Update `custom_v2.json`
6. Regenerate payloads, requester methods, and adaptors
7. Add serde fixtures for new or changed wire shapes
8. Audit every new `InputFile` path for multipart handling
9. Add focused unit tests
10. Add a wire-level test when behavior crosses serialization or transport boundaries
11. Run the full checks
12. State remaining gaps explicitly in the PR description

Do not use the current live API documentation as the sole source when implementing an older target version. Later changes can make the implementation accidentally incompatible with the requested version.

## Types and serde

New public Telegram types should normally implement:

```text
Clone
Debug
PartialEq
Serialize
Deserialize
schemars::JsonSchema under cfg(test)
```

Also derive `Eq` and `Hash` when their semantics are truthful and every field supports them.

Do not derive or manually implement equality that ignores semantically relevant media or file fields merely to satisfy an existing bound.

Use dedicated ID newtypes instead of raw integers or strings where the repository already follows that convention.

For tagged unions:

- match the Telegram discriminator exactly
- prefer explicit enum variants
- preserve unknown/future-compatible behavior only when the surrounding API already does so intentionally
- test every new variant with representative JSON
- test malformed and ambiguous shapes for custom deserializers

Custom serde code is high risk. Review field precedence, unknown fields, missing discriminators, duplicate fields, and fallback behavior.

## Request payloads and method signatures

For every changed method, verify:

- Rust required arguments match Telegram required arguments
- optional setters serialize to the exact Telegram field names
- integer types can represent the documented range
- `Into` and `collect` conversions are appropriate
- return type matches the API
- inline and non-inline sibling methods remain consistent
- all requester adaptors receive the new method or parameter
- generated code remains deterministic

Avoid narrowing integer types merely because current examples are small. Use the smallest type that covers the full documented range.

## Multipart and `InputFile`

Any payload containing a local file, bytes, or a nested `InputFile` must use multipart transport.

A correct multipart implementation must preserve this invariant:

```text
Every serialized attach://<id> has exactly one multipart file part named <id>,
and every multipart file part is referenced by exactly one intended attach://<id>.
```

When adding media types, inspect all nested file-bearing fields, including:

- main media
- thumbnail
- cover
- paired live-photo files
- sticker files
- files inside arrays or enum variants
- files inside nested options or rich-message blocks

Tests that only count traversal callbacks are useful but not sufficient for transport-critical code. Add a wire-level integration test when a new request shape reaches `reqwest::multipart::Form`.

For multipart changes, test both immutable and mutable traversal where both APIs exist.

## Renderer and rich content

Renderer changes must not silently discard supported Telegram semantics.

When adding an entity or rich node:

- implement all supported target formats
- add tests for optional attributes being present and absent
- add nested-entity tests when nesting is legal
- escape text and attribute values correctly
- document any lossy fallback

For structures that cannot be represented in HTML or MarkdownV2, return or record an explicit loss/fallback result rather than pretending the conversion is exact.

Keep wire-model work separate from presentation helpers when possible. The API types must remain usable even if a high-level renderer does not support every feature yet.

## Testing expectations

### Fast local loop

```shell
just fmt
just lint
just test
```

### Repository local CI approximation

```shell
just ci
```

`just ci` is useful but is not the whole GitHub matrix.

### Required before a substantial PR

```shell
cargo fmt --all -- --check

cargo test -p teloxide-core \
  --features "full nightly" \
  codegen \
  -- --nocapture

cargo test --workspace --features "full nightly"

cargo clippy --workspace \
  --all-targets \
  --features "full nightly" \
  -- -D warnings

cargo check --workspace --examples --features full
cargo check --workspace --no-default-features

cargo docs
```

The pinned default toolchain is defined in `rust-toolchain.toml`. Do not silently update it as part of an unrelated task.

For API, serde, or multipart changes, add focused tests that fail for the original bug. Do not rely only on compilation or broad workspace tests.

A complete GitHub CI run should cover:

- formatting
- clippy with warnings denied
- stable
- beta
- pinned nightly
- MSRV
- unit tests
- integration tests
- documentation tests
- examples
- no-default-features
- rustdoc

## Current integration direction

`master` is the release line. Use the latest immutable package tags to
identify the released code; do not treat the current branch head as a release
without a matching tag and version entry. The next API update targets Bot API
10.2 and should be integrated on `next` before a versioned promotion to
`master`.

Do not state complete Bot API 10.0 coverage without an independent external audit against the official documentation. Treat the 10.0 coverage claim as qualified until that audit is recorded.

For the 10.2 update, separate work into reviewable layers:

1. API/object audit
2. leaf types and IDs
3. recursive rich-text types
4. rich block types
5. rich media and multipart traversal
6. methods and changed parameters
7. serde fixtures
8. renderers and lossy fallback reporting
9. final external diff

Do not combine the entire 10.2 update and a renderer rewrite into one unreviewable commit.

## Code style

Follow `CODE_STYLE.md`.

Particularly:

- put generic bounds in `where` clauses
- use `Self` where appropriate
- prefer `.to_owned()` for `&str` to `String`
- group imports in repository order
- use full paths for logging macros
- write documentation about what code does, not a narration of implementation
- use the spelling `teloxide`, `teloxide-core`, and `teloxide-macros`
- use `#[must_use]` for pure result-producing functions

Run rustfmt instead of manually imitating its output.

## Scope discipline

Keep commits focused.

Good separation examples:

- schema and generated method update
- new API types and serde fixtures
- multipart traversal fix and transport tests
- renderer support and renderer tests
- documentation or agent instructions

Avoid mixing unrelated dependency upgrades, formatting churn, renames, renderer redesigns, and API additions in one commit.

Do not change existing public behavior outside the task scope without calling it out explicitly.

## Commit and PR requirements

Use clear imperative commit subjects, for example:

```text
feat: add Bot API 10.2 rich message types
fix: preserve video covers in multipart requests
test: verify sendPoll attachment mapping
docs: document agent development workflow
```

PR descriptions must include:

- what changed
- why it changed
- official API version or source when relevant
- generated files affected
- multipart implications
- tests added
- commands run
- known omissions or follow-up work

Do not claim a check was run unless it actually completed successfully.

Do not merge a PR merely because codegen and compilation are green. For transport and serde changes, require behavior tests.

For release/hotfix PRs targeting `master`, verify the release/hotfix version
bump, `CHANGELOG.md` entry, target package tags, and the exact merge-commit tag
plan. For an explicitly authorized governance PR, verify that it makes no
crate-release claim, does not require a package version bump, and receives no
package tag.

## Agent reporting format

When finishing a task, report:

1. Summary of changes
2. Files or subsystems touched
3. Tests and commands run with outcomes
4. Remaining risks or unverified assumptions
5. Recommended next step

When blocked, provide the exact error, file, and smallest reproducer. Do not hide uncertainty behind a confident summary.

## Definition of done

A task is done only when:

- the implementation matches the intended Telegram API version
- both local schemas are synchronized when methods changed
- generated output is regenerated and reviewed
- serde behavior is covered for changed wire shapes
- multipart invariants are tested for new file-bearing paths
- focused regression tests exist
- formatting and clippy pass
- relevant workspace and feature combinations pass
- documentation and changelog are updated when user-visible behavior changed
- ordinary feature PRs target `next`; versioned release/hotfix PRs and explicitly authorized governance PRs target `master`
- every code promotion to `master` has the appropriate package version bump, changelog entry, and immutable tag plan; governance promotions explicitly have none of these release artifacts
- governance commits merged to `master` are reconciled into `next` before ordinary feature work resumes
- remaining gaps are stated explicitly

Correctness at the Telegram wire boundary is more important than making local schemas, derives, or generated code look convenient.
