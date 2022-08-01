# Stroming

A set of traits for reading from and writing to a stream store, together with an in-memory implementation. This project is mostly academic.

[View docs](https://docs.rs/stroming)
[View crate](https://crates.io/crates/stroming)

# Use

```toml
[dependencies]
stroming = "0.0.9"
```

```rust
let mut store = MemoryStreamStore::new();
let data = r#"{"test": "data"}"#.as_bytes().to_vec();
let msg = Message {
    message_type: "TestMessage".to_owned(),
    data,
    metadata: vec![],
};

let _ = store.write_to_stream("TestStream-1", StreamVersion::NoStream, &[msg]);

let (version, messages) = store.read_from_stream("TestStream-1", ReadDirection::Forwards);

assert_eq!(version, StreamVersion::Revision(0));
assert_eq!(messages.len(), 1);
assert_eq!(messages[0].message_type, "TestMessage");
```

# License

MIT
