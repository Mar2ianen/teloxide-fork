# Release procedure

This fork releases the workspace from `next` into `master`. Do not publish
from a feature branch or from a worktree that is ahead of `origin/next`.

## Release preparation

1. Fetch `origin` and create a clean `release/<version>` branch from the exact
   `origin/next` commit.
2. Confirm the release number for every changed crate. New public features are
   minor releases under SemVer; for the outbound scheduler this is
   `teloxide 0.19.0`, `teloxide-core 0.15.0` and `teloxide-macros 0.11.1`.
3. Update `CHANGELOG.md`, the core and macros changelogs, `MIGRATION_GUIDE.md`
   and user-facing examples.
4. Run formatting, the complete workspace test/lint matrix, documentation
   builds and `cargo package --list`/`cargo package` for every publishable
   crate.
5. Open a release pull request against `master` and wait for every required
   status check.

## Publishing

Publish in dependency order after the release pull request has merged:

```bash
cargo publish --package teloxide-core
cargo publish --package teloxide-macros
cargo publish --package teloxide
```

Use `--dry-run` first. The `teloxide` manifest must contain versioned path
dependencies for the released `teloxide-core` and `teloxide-macros` packages.
Wait for each dependency to become available on crates.io before publishing the
next package.

Create immutable annotated tags on the exact release commit:

```bash
GIT_EDITOR=true git tag -a v0.19.0 <release-commit> -m "Release teloxide 0.19.0"
GIT_EDITOR=true git tag -a core-v0.15.0 <release-commit> -m "Release teloxide-core 0.15.0"
GIT_EDITOR=true git tag -a macros-v0.11.1 <release-commit> -m "Release teloxide-macros 0.11.1"
git push origin v0.19.0 core-v0.15.0 macros-v0.11.1
```

Publish the GitHub release with the root changelog entry and link the
corresponding crates.io versions. Keep the previous release available for
rollback; enabling `Bot::outbound` in an application remains an explicit
rollout decision.
