# Development

To run plugin with `solana-test-validator`:

```
cp config.json config.dev.json && vim config.dev.json
cargo build -p richat-plugin-agave --lib --release --features="rustls-install-default-provider" && solana-test-validator --geyser-plugin-config plugin-agave/config.dev.json
```

If you run plugin on mainnet validator do not try to do it in `debug` mode, validator would start fall behind.

Bank-aware notifications preserve the validator's source-local bank IDs. The
`block_footer` and `entry_update_parent` options are disabled by default. Enabling
them requires consumers using the Yellowstone gRPC proto 14 schema, including
typed Alpenglow certificates. Entry parent markers follow the entry subscription
filter. Invalid finalization signatures omit the final certificate while the
remaining footer fields are still sent.
