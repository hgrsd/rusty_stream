use std::collections::HashMap;
use std::sync::RwLock;

use uuid::Uuid;

use crate::stream::{
    Message, ReadDirection, ReadFromStream, Stream, StreamMessage, StreamVersion, WriteResult,
    WriteToStream,
};

type Streams = HashMap<String, Vec<StreamMessage>>;

struct MemoryStreamStore {
    streams: RwLock<Streams>,
}

impl MemoryStreamStore {
    pub fn new() -> Self {
        Self {
            streams: RwLock::new(HashMap::new()),
        }
    }

    fn get_stream_version(&self, stream: &Stream) -> StreamVersion {
        if let Some(event) = stream.last() {
            StreamVersion::Revision(event.revision)
        } else {
            StreamVersion::NoStream
        }
    }
}

impl ReadFromStream for MemoryStreamStore {
    fn read_from_stream(
        &self,
        stream_name: &str,
        direction: ReadDirection,
    ) -> (StreamVersion, Stream) {
        let lock = self.streams.read().unwrap();
        let stream = lock.get(stream_name).map_or(Vec::new(), |x| x.clone());
        let version = self.get_stream_version(&stream);

        let iter = stream.into_iter();

        let stream = if direction == ReadDirection::Forwards {
            iter.collect()
        } else {
            iter.rev().collect()
        };

        (version, stream)
    }
}

impl WriteToStream for MemoryStreamStore {
    fn write_to_stream(
        &mut self,
        stream_name: &str,
        expected_version: StreamVersion,
        message: Message,
    ) -> WriteResult {
        let mut locked_streams = self.streams.write().unwrap();
        let stream = locked_streams.get_mut(stream_name);

        let version = match stream {
            Some(ref s) => self.get_stream_version(s),
            None => StreamVersion::NoStream,
        };

        if version != expected_version {
            return WriteResult::WrongExpectedVersion;
        }

        let revision = match version {
            StreamVersion::NoStream => 0,
            StreamVersion::Revision(n) => n + 1,
        };

        let m = StreamMessage {
            id: Uuid::new_v4().to_string(),
            message_type: message.message_type,
            data: message.data,
            revision,
        };

        if let Some(existing_messages) = stream {
            existing_messages.push(m);
        } else {
            locked_streams.insert(stream_name.to_owned(), vec![m]);
        };

        WriteResult::Ok(StreamVersion::Revision(revision))
    }
}

#[cfg(test)]
mod test {
    use super::MemoryStreamStore;
    use crate::stream::{
        Message, ReadDirection, ReadFromStream, StreamVersion, WriteResult, WriteToStream,
    };

    #[test]
    fn it_reads_events_forwards() {
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

        let (version, messages) = store.read_from_stream("test stream", ReadDirection::Forwards);

        assert_eq!(version, StreamVersion::Revision(1));
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].message_type, "TestMessage");
        assert_eq!(messages[1].message_type, "AnotherMessage");
    }

    #[test]
    fn it_reads_events_backwards() {
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

        let (version, messages) = store.read_from_stream("test stream", ReadDirection::Backwards);

        assert_eq!(version, StreamVersion::Revision(1));
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].message_type, "AnotherMessage");
        assert_eq!(messages[1].message_type, "TestMessage");
    }

    #[test]
    fn it_handles_wrong_version() {
        let data = r#"{"test": "data"}"#.as_bytes().to_vec();
        let msg = Message {
            message_type: "TestMessage".to_owned(),
            data: data.clone(),
        };
        let mut store = MemoryStreamStore::new();
        store.write_to_stream("test stream", StreamVersion::NoStream, msg.clone());

        let append_result = store.write_to_stream("test stream", StreamVersion::NoStream, msg);
        assert_eq!(append_result, WriteResult::WrongExpectedVersion);

        let (version, messages) = store.read_from_stream("test stream", ReadDirection::Forwards);
        assert_eq!(messages.len(), 1);
        assert_eq!(version, StreamVersion::Revision(0));
    }
}
