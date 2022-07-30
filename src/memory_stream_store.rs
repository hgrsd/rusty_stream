use std::collections::HashMap;
use std::sync::{RwLock};

use uuid::Uuid;

use crate::stream::{Message, ReadStream, StreamMessage, StreamVersion, WriteResult, WriteToStream, Stream};
use crate::stream::StreamVersion::{NoStream, Revision};

type Streams = HashMap<String, Vec<StreamMessage>>;

struct MemoryStreamStore {
    streams: RwLock<Streams>,
}

impl MemoryStreamStore {
    fn new() -> Self {
        Self {
            streams: RwLock::new(HashMap::new()),
        }
    }

    fn get_stream_version(&self, stream: Option<&Stream>) -> StreamVersion {
        let last_event = stream.and_then(|s| { s.last() });
        if let Some(event) = last_event {
            Revision(event.revision)
        } else {
            NoStream
        }
    }
}

impl ReadStream for MemoryStreamStore {
    fn read_stream(&self, stream_name: &str, max: Option<usize>) -> (StreamVersion, Stream) {
        let lock = self.streams.read().unwrap();
        let stream = lock.get(stream_name);
        let version = self.get_stream_version(stream);
        if let NoStream = version {
            return (NoStream, Vec::new());
        }
        match (stream, max) {
            (None, _) => (version, Vec::new()),
            (Some(s), None) => (version, s.clone()),
            (Some(s), Some(n)) => {
                let take = std::cmp::min(n, s.len());
                let mut vec = Vec::new();
                for idx in 0..take {
                    vec.push(s[idx].clone());
                }
                (version, vec)
            }
        }
    }
}

impl WriteToStream for MemoryStreamStore {
    fn write_to_stream(&mut self, stream_name: &str, expected_version: StreamVersion, message: Message) -> WriteResult {
        let mut locked_streams = self.streams.write().unwrap();

        let version = self.get_stream_version(locked_streams.get(stream_name));
        if version != expected_version {
            return WriteResult::WrongExpectedVersion;
        }

        let new_version = match version {
            NoStream => 0,
            Revision(x) => x + 1,
        };
        let m = StreamMessage {
            id: Uuid::new_v4().to_string(),
            message_type: message.message_type.clone(),
            data: message.data,
            revision: new_version,
        };

        match locked_streams.get_mut(stream_name) {
            None => {
                locked_streams.insert(stream_name.to_owned(), vec![m]);
            }
            Some(messages) => {
                messages.push(m);
            }
        };

        WriteResult::Ok(Revision(new_version))
    }
}

#[cfg(test)]
mod test {
    use crate::memory_stream_store::MemoryStreamStore;
    use crate::stream::{Message, ReadStream, StreamVersion, WriteResult, WriteToStream};

    #[test]
    fn it_can_write_and_read() {
        let data = r#"{"test": "data"}"#.as_bytes().to_vec();
        let msg = Message {
            message_type: "TestMessage".to_owned(),
            data: data.clone(),
        };
        let mut store = MemoryStreamStore::new();

        let append_result = store.write_to_stream("test stream", StreamVersion::NoStream, msg);
        assert_eq!(append_result, WriteResult::Ok(StreamVersion::Revision(0)));

        let (version, messages) = store.read_stream("test stream", None);
        assert_eq!(version, StreamVersion::Revision(0));
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].data, data);
        assert_eq!(messages[0].message_type, "TestMessage");
    }

    #[test]
    fn it_can_write_multiple_events() {
        let data = r#"{"test": "data"}"#.as_bytes().to_vec();
        let msg = Message {
            message_type: "TestMessage".to_owned(),
            data: data.clone(),
        };
        let mut store = MemoryStreamStore::new();
        let _ = store.write_to_stream("test stream", StreamVersion::NoStream, msg);
        let data = r#"{"test2": "data2"}"#.as_bytes().to_vec();
        let msg = Message {
            message_type: "AnotherMessage".to_owned(),
            data: data.clone(),
        };
        let _ = store.write_to_stream("test stream", StreamVersion::Revision(0), msg);

        let (version, messages) = store.read_stream("test stream", None);

        assert_eq!(version, StreamVersion::Revision(1));
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].message_type, "TestMessage");
        assert_eq!(messages[1].message_type, "AnotherMessage");
    }

    #[test]
    fn it_does_not_append_when_wrong_version() {
        let data = r#"{"test": "data"}"#.as_bytes().to_vec();
        let msg = Message {
            message_type: "TestMessage".to_owned(),
            data: data.clone(),
        };
        let mut store = MemoryStreamStore::new();
        store.write_to_stream("test stream", StreamVersion::NoStream, msg.clone());

        let append_result = store.write_to_stream("test stream", StreamVersion::NoStream, msg);
        assert_eq!(append_result, WriteResult::WrongExpectedVersion);

        let (version, messages) = store.read_stream("test stream", None);
        assert_eq!(messages.len(), 1);
        assert_eq!(version, StreamVersion::Revision(0));
    }
}