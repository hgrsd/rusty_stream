use std::collections::HashMap;

use uuid::Uuid;

use crate::stream::{Message, ReadStream, StreamMessage, StreamVersion, WriteResult, WriteToStream};
use crate::stream::StreamVersion::{NoStream, Revision};

struct MemoryStreamStore {
    streams: HashMap<String, Vec<StreamMessage>>,
}

impl MemoryStreamStore {
    fn new() -> Self {
        Self {
            streams: HashMap::new(),
        }
    }

    fn get_stream_version(&self, stream: &str) -> StreamVersion {
        let last_event = self.streams.get(stream).and_then(|s| { s.last() });
        if let Some(event) = last_event {
            Revision(event.revision)
        } else {
            NoStream
        }
    }

    fn get_messages(&self, stream: &str) -> &[StreamMessage] {
        match self.streams.get(stream) {
            None => &[],
            Some(msg) => msg
        }
    }
}

impl ReadStream for MemoryStreamStore {
    fn read_stream(&self, stream_name: &str, max: Option<usize>) -> (StreamVersion, &[StreamMessage]) {
        let version = self.get_stream_version(stream_name);
        if let NoStream = version {
            return (NoStream, &[]);
        }
        match max {
            None => (version, self.get_messages(stream_name)),
            Some(n) => {
                let msg = self.get_messages(stream_name);
                let take = std::cmp::min(n, msg.len());
                (version, &msg[0..take - 1])
            },
        }
    }
}

impl WriteToStream for MemoryStreamStore {
    fn write_to_stream(&mut self, stream_name: &str, expected_version: StreamVersion, message: Message) -> WriteResult {
        let version = self.get_stream_version(stream_name);
        if version != expected_version {
            WriteResult::WrongExpectedVersion
        } else {
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

            match self.streams.get_mut(stream_name) {
                None => {
                    self.streams.insert(stream_name.to_owned(), vec![m]);
                },
                Some(messages) => {
                    messages.push(m);
                }
            };
            WriteResult::Ok(Revision(new_version))
        }
    }
}

#[cfg(test)]
mod test {
    use crate::memory_stream_store::MemoryStreamStore;
    use crate::stream::{Message, WriteResult, WriteToStream};
    use crate::stream::StreamVersion::{NoStream, Revision};
    use crate::stream::WriteResult::WrongExpectedVersion;

    #[test]
    fn it_can_write_and_read() {
        let data = r#"{"test": "data"}"#.as_bytes().to_vec();
        let msg = Message {
            message_type: "TestMessage".to_owned(),
            data: data.clone(),
        };
        let mut store = MemoryStreamStore::new();
        let append_result = store.write_to_stream("test stream", NoStream, msg);
        assert_eq!(append_result, WriteResult::Ok(Revision(0)));
        let read = store.get_messages("test stream");
        assert_eq!(read.len(), 1);
        assert_eq!(read[0].data, data);
        assert_eq!(read[0].message_type, "TestMessage");
    }

    #[test]
    fn it_does_not_append_when_wrong_version() {
        let data = r#"{"test": "data"}"#.as_bytes().to_vec();
        let msg = Message {
            message_type: "TestMessage".to_owned(),
            data: data.clone(),
        };
        let mut store = MemoryStreamStore::new();
        store.write_to_stream("test stream", NoStream, msg.clone());
        let append_result = store.write_to_stream("test stream", NoStream, msg);
        assert_eq!(append_result, WrongExpectedVersion);
        let read = store.get_messages("test stream");
        assert_eq!(read.len(), 1);
    }
}