# Release procedure

This fork releases the reviewed workspace from `next` into `master`. Published
crate versions and immutable tags are the release authority. Do not publish
from a feature branch or directly from an untagged worktree.

## Release preparation

1. Fetch `origin` and create a clean `release/<release-id>` branch from the
   exact reviewed `origin/next` commit.
2. Choose an independent semver for every changed crate. For Bot API 10.4, the
   first release candidate is `teloxide 0.21.0-rc.1`,
   `teloxide-core 0.17.0-rc.1` and `teloxide-macros 0.11.2-rc.1`. The first
   two crates take a minor bump for the breaking/public additions in the 0.x
   line; the macro bug fix takes a patch bump.
3. Update each affected manifest, path dependency version, lockfile, changelog,
   `MIGRATION_GUIDE.md`, README install examples and release notes.
4. Run formatting, the complete workspace test/lint matrix, documentation
   builds and `cargo package --list` plus `cargo package --dry-run` for every
   publishable crate. Validate package dependencies in dependency order.
5. Open a versioned release pull request against `master` with the target
   versions, Bot API version, package impact and exact tag plan. Wait for every
   required status check before merging.

## Release candidate tags

After the release pull request has merged, create immutable annotated tags on
the exact merge commit. For RC1, the tag plan is:

```bash
GIT_EDITOR=true git tag -a v0.21.0-rc.1 <release-commit> -m "Release teloxide 0.21.0-rc.1"
GIT_EDITOR=true git tag -a core-v0.17.0-rc.1 <release-commit> -m "Release teloxide-core 0.17.0-rc.1"
GIT_EDITOR=true git tag -a macros-v0.11.2-rc.1 <release-commit> -m "Release teloxide-macros 0.11.2-rc.1"
git push origin v0.21.0-rc.1 core-v0.17.0-rc.1 macros-v0.11.2-rc.1
```

Never move or reuse a published tag. Each later RC increments the prerelease
number and points to its own reviewed release merge commit.

## Publishing

Publish only from the tagged release commit, in dependency order. The
`cargo publish --dry-run` checks above validate the packages without uploading
them. Once the release tags are in place, publish `teloxide-core`, then
`teloxide-macros`, then `teloxide`, waiting for each dependency to become
available on crates.io before publishing the next package. The `teloxide` manifest must contain
versioned dependencies for the candidate core and macros packages.

Publish the GitHub release as a prerelease with the root changelog entry and
links to the matching crates.io versions. Keep the previous stable release
available for rollback; enabling `Bot::outbound` in an application remains an
explicit rollout decision.
