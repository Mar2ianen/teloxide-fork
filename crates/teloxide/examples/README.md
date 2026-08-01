# Usage

```
$ RUST_LOG=info cargo run --features="full" --example <example-name>
```

Don't forget to initialise the `TELOXIDE_TOKEN` environmental variable.

The `drafter` example demonstrates a native private-chat draft, explicit
`flush` calls for two visible preview updates and final delivery. Set
`TELOXIDE_USER_ID` to a real private-chat user ID when running it. Production
applications should share one `InProcessRateLimiter` across all drafters using
the same bot token.
