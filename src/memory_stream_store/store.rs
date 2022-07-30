use std::collections::HashMap;
use std::sync::{RwLock, RwLockWriteGuard};

use uuid::Uuid;

use crate::memory_stream_store::index::LogPositionIndex;
use crate::stream::{
    Message, MessagePosition, ReadDirection, ReadFromCategory, ReadFromStream, Stream,
    StreamMessage, StreamVersion, WriteResult, WriteToStream,
};

pub struct MemoryStreamStore {
    log: RwLock<Vec<StreamMessage>>,
    streams: RwLock<LogPositionIndex>,
    categories: RwLock<LogPositionIndex>,
    stream_metadata: RwLock<HashMap<String, usize>>,
}

impl MemoryStreamStore {
    pub fn new() -> Self {
        Self {
            log: RwLock::new(Vec::new()),
            streams: RwLock::new(LogPositionIndex::new()),
            categories: RwLock::new(LogPositionIndex::new()),
            stream_metadata: RwLock::new(HashMap::new()),
        }
    }

    fn do_write(
        mut log: RwLockWriteGuard<Vec<StreamMessage>>,
        mut stream_index: RwLockWriteGuard<LogPositionIndex>,
        mut category_index: RwLockWriteGuard<LogPositionIndex>,
        mut stream_metadata: RwLockWriteGuard<HashMap<String, usize>>,
        stream_name: &str,
        event: StreamMessage,
    ) -> WriteResult {
        let pos = event.position.clone();
        log.push(event);

        stream_index.write_position(stream_name, pos.position);

        let category = stream_name
            .split('-')
            .nth(0)
            .expect("No category can be inferred from stream");
        category_index.write_position(category, pos.position);

        stream_metadata.insert(stream_name.to_owned(), pos.revision);
        WriteResult::Ok(pos)
    }
}

impl ReadFromStream for MemoryStreamStore {
    fn read_from_stream(
        &self,
        stream_name: &str,
        direction: ReadDirection,
    ) -> (StreamVersion, Stream) {
        let log = self.log.read().unwrap();
        let index = self.streams.read().unwrap();
        let log_positions = index.get_positions(stream_name);

        let mut stream_version = StreamVersion::NoStream;
        let mut messages = Vec::with_capacity(log_positions.len() - 1);
        for position in log_positions {
            let message = log.get(*position).unwrap();
            stream_version = StreamVersion::Revision(message.position.revision);
            messages.push(message.clone());
        }

        let stream = if direction == ReadDirection::Forwards {
            messages
        } else {
            messages.into_iter().rev().collect()
        };

        (stream_version, stream)
    }
}

impl ReadFromCategory for MemoryStreamStore {
    fn read_from_category(
        &mut self,
        category_name: &str,
        offset: usize,
        max_messages: Option<usize>,
    ) -> Stream {
        let log = self.log.read().unwrap();
        let index = self.categories.read().unwrap();
        let log_positions = index.get_positions_after(category_name, offset);
        let n = max_messages.unwrap_or(log_positions.len());
        let mut messages = Vec::with_capacity(n);
        for position in log_positions.iter().take(n) {
            messages.push(log.get(*position).unwrap().clone());
        }
        messages
    }
}

impl WriteToStream for MemoryStreamStore {
    fn write_to_stream(
        &mut self,
        stream_name: &str,
        expected_version: StreamVersion,
        message: Message,
    ) -> WriteResult {
        let log = self.log.write().unwrap();
        let streams = self.streams.write().unwrap();
        let categories = self.categories.write().unwrap();
        let stream_metadata = self.stream_metadata.write().unwrap();

        let version = stream_metadata
            .get(stream_name)
            .map(|revision| StreamVersion::Revision(revision.clone()))
            .unwrap_or(StreamVersion::NoStream);

        if version != expected_version {
            return WriteResult::WrongExpectedVersion;
        }

        let new_revision = match version {
            StreamVersion::NoStream => 0,
            StreamVersion::Revision(n) => n + 1,
        };

        let new_position = MessagePosition {
            revision: new_revision,
            position: log.len(),
        };

        let m = StreamMessage {
            id: Uuid::new_v4().to_string(),
            message_type: message.message_type,
            data: message.data,
            position: new_position.clone(),
        };

        MemoryStreamStore::do_write(log, streams, categories, stream_metadata, stream_name, m)
    }
}

#[cfg(test)]
mod test {
    use super::MemoryStreamStore;
    use crate::stream::{
        Message, ReadDirection, ReadFromCategory, ReadFromStream, StreamVersion, WriteResult,
        WriteToStream,
    };

    #[test]
    fn it_reads_events_forwards() {
        let data = r#"{"test": "data"}"#.as_bytes().to_vec();
        let msg = Message {
            message_type: "TestMessage".to_owned(),
            data: data.clone(),
        };
        let mut store = MemoryStreamStore::new();
        let _ = store.write_to_stream("TestStream-1", StreamVersion::NoStream, msg);
        let data = r#"{"test2": "data2"}"#.as_bytes().to_vec();
        let msg = Message {
            message_type: "AnotherMessage".to_owned(),
            data: data.clone(),
        };
        let _ = store.write_to_stream("TestStream-1", StreamVersion::Revision(0), msg);

        let (version, messages) = store.read_from_stream("TestStream-1", ReadDirection::Forwards);

        assert_eq!(version, StreamVersion::Revision(1));
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].message_type, "TestMessage");
        assert_eq!(messages[1].message_type, "AnotherMessage");
    }

    #[test]
    fn it_reads_a_category() {
        let data = r#"{"test": "data"}"#.as_bytes().to_vec();
        let msg = Message {
            message_type: "TestMessage".to_owned(),
            data: data.clone(),
        };
        let mut store = MemoryStreamStore::new();
        store.write_to_stream("TestStream-1", StreamVersion::NoStream, msg);

        let data = r#"{"test2": "data2"}"#.as_bytes().to_vec();
        let msg = Message {
            message_type: "AnotherMessage".to_owned(),
            data: data.clone(),
        };
        store.write_to_stream("TestStream-1", StreamVersion::Revision(0), msg);

        let data = r#"{"test3": "data3"}"#.as_bytes().to_vec();
        let msg = Message {
            message_type: "A third message".to_owned(),
            data: data.clone(),
        };
        store.write_to_stream("TestStream-2", StreamVersion::NoStream, msg);

        let msg = Message {
            message_type: "A fourth message".to_owned(),
            data: data.clone(),
        };
        store.write_to_stream("DifferentCategory", StreamVersion::NoStream, msg);

        let messages = store.read_from_category("TestStream", 0, None);
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0].message_type, "TestMessage");
        assert_eq!(messages[1].message_type, "AnotherMessage");
        assert_eq!(messages[2].message_type, "A third message");
    }

    #[test]
    fn it_reads_a_category_max_messages() {
        let data = r#"{"test": "data"}"#.as_bytes().to_vec();
        let msg = Message {
            message_type: "TestMessage".to_owned(),
            data: data.clone(),
        };
        let mut store = MemoryStreamStore::new();
        store.write_to_stream("TestStream-1", StreamVersion::NoStream, msg);

        let data = r#"{"test3": "data3"}"#.as_bytes().to_vec();
        let msg = Message {
            message_type: "A second message".to_owned(),
            data: data.clone(),
        };
        store.write_to_stream("TestStream-2", StreamVersion::NoStream, msg);

        let msg = Message {
            message_type: "A third message".to_owned(),
            data: data.clone(),
        };
        store.write_to_stream("TestStream-1", StreamVersion::Revision(0), msg);

        let messages = store.read_from_category("TestStream", 0, Some(2));
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].message_type, "TestMessage");
        assert_eq!(messages[1].message_type, "A second message");
    }

    #[test]
    fn it_reads_a_category_from_offset() {
        let data = r#"{"test": "data"}"#.as_bytes().to_vec();
        let msg = Message {
            message_type: "TestMessage".to_owned(),
            data: data.clone(),
        };
        let mut store = MemoryStreamStore::new();
        store.write_to_stream("TestStream-1", StreamVersion::NoStream, msg);

        let data = r#"{"test3": "data3"}"#.as_bytes().to_vec();
        let msg = Message {
            message_type: "A second message".to_owned(),
            data: data.clone(),
        };

        let global_position =
            match store.write_to_stream("TestStream-2", StreamVersion::NoStream, msg) {
                WriteResult::Ok(position) => position.position,
                _ => unreachable!(),
            };

        let msg = Message {
            message_type: "A third message".to_owned(),
            data: data.clone(),
        };
        store.write_to_stream("TestStream-1", StreamVersion::Revision(0), msg);

        let messages = store.read_from_category("TestStream", global_position, Some(2));
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].message_type, "A second message");
        assert_eq!(messages[1].message_type, "A third message");
    }

    #[test]
    fn it_reads_events_backwards() {
        let data = r#"{"test": "data"}"#.as_bytes().to_vec();
        let msg = Message {
            message_type: "TestMessage".to_owned(),
            data: data.clone(),
        };
        let mut store = MemoryStreamStore::new();
        let _ = store.write_to_stream("TestStream-1", StreamVersion::NoStream, msg);
        let data = r#"{"test2": "data2"}"#.as_bytes().to_vec();
        let msg = Message {
            message_type: "AnotherMessage".to_owned(),
            data: data.clone(),
        };
        let _ = store.write_to_stream("TestStream-1", StreamVersion::Revision(0), msg);

        let (version, messages) = store.read_from_stream("TestStream-1", ReadDirection::Backwards);

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
        store.write_to_stream("TestStream-1", StreamVersion::NoStream, msg.clone());

        let append_result = store.write_to_stream("TestStream-1", StreamVersion::NoStream, msg);
        assert_eq!(append_result, WriteResult::WrongExpectedVersion);

        let (version, messages) = store.read_from_stream("TestStream-1", ReadDirection::Forwards);
        assert_eq!(messages.len(), 1);
        assert_eq!(version, StreamVersion::Revision(0));
    }
}
