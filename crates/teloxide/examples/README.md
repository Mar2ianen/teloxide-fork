# Usage

```
$ RUST_LOG=info cargo run --features="full" --example <example-name>
```

Don't forget to initialise the `TELOXIDE_TOKEN` environmental variable.

The `drafter` example demonstrates a native private-chat draft, synchronous
latest-wins updates and explicit final delivery. Production applications
should share one `InProcessRateLimiter` across all drafters using the same bot
token.
