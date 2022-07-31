use std::collections::HashMap;
use std::sync::{RwLock, RwLockWriteGuard};

use uuid::Uuid;

use crate::memory_stream_store::index::LogPositionIndex;
use crate::store::{
    Message, MessagePosition, ReadDirection, ReadFromCategory, ReadFromStream, Stream,
    StreamMessage, StreamVersion, WriteResult, WriteToStream,
};

/// An in-memory implementation of a stream store.
///
/// The architecture of this in-memory store is as follows:
///
/// *  There is a single append-only log that owns all messages in the store (a vector of
/// StreamMessages). All
/// writes are made to this log, which is guarded by a read-write lock. This
/// means that there should be a guaranteed global order between all messages in the store.
///
/// * There are two LogPositionIndices, one for streams and one for categories. Querying these
/// indices will return an array of pointers into the global message log. Under the hood, these
/// indices are represented by a HashMap with the stream or category name as its keys, and a vector
/// of usizes as its values. Each usize is an index into the global log.
///
/// * There is a HashMap that keeps track of stream revisions for fast lookups, to be used for
/// detection of version conflits.
///
/// * Each write into the store takes out write locks on the global log, the indices, and the map
/// of stream revisions. This essentially makes this in-memory store a single writer store.
/// However,
/// the RwLock does allow for concurrent reads.
pub struct MemoryStreamStore {
    log: RwLock<Vec<StreamMessage>>,
    streams: RwLock<LogPositionIndex>,
    categories: RwLock<LogPositionIndex>,
    stream_revisions: RwLock<HashMap<String, usize>>,
}

impl MemoryStreamStore {
    pub fn new() -> Self {
        Self {
            log: RwLock::new(Vec::new()),
            streams: RwLock::new(LogPositionIndex::new()),
            categories: RwLock::new(LogPositionIndex::new()),
            stream_revisions: RwLock::new(HashMap::new()),
        }
    }

    fn do_write(
        log: &mut RwLockWriteGuard<Vec<StreamMessage>>,
        stream_index: &mut RwLockWriteGuard<LogPositionIndex>,
        category_index: &mut RwLockWriteGuard<LogPositionIndex>,
        stream_metadata: &mut RwLockWriteGuard<HashMap<String, usize>>,
        stream_name: &str,
        event: StreamMessage,
    ) -> WriteResult {
        let pos = event.position.clone();
        log.push(event);

        stream_index.write_position(stream_name, pos.position);

        let category = stream_name
            .split('-')
            .next()
            .expect("No category can be inferred from stream");
        category_index.write_position(category, pos.position);

        stream_metadata.insert(stream_name.to_owned(), pos.revision);
        WriteResult::Ok(pos)
    }
}

impl Default for MemoryStreamStore {
    fn default() -> Self {
        Self::new()
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
        messages: &[Message],
    ) -> WriteResult {
        let mut log = self.log.write().unwrap();
        let mut streams = self.streams.write().unwrap();
        let mut categories = self.categories.write().unwrap();
        let mut stream_metadata = self.stream_revisions.write().unwrap();

        let version = stream_metadata
            .get(stream_name)
            .map(|revision| StreamVersion::Revision(*revision))
            .unwrap_or(StreamVersion::NoStream);

        if version != expected_version {
            return WriteResult::WrongExpectedVersion;
        }

        let next_rev = match version {
            StreamVersion::NoStream => 0,
            StreamVersion::Revision(n) => n + 1,
        };

        let mut next_pos = MessagePosition {
            revision: next_rev,
            position: log.len(),
        };

        let mut append_result = WriteResult::Ok(next_pos);
        for  message in messages {
            let m = StreamMessage {
                id: Uuid::new_v4().to_string(),
                message_type: message.message_type.clone(),
                data: message.data.clone(),
                position: next_pos,
            };
            append_result = MemoryStreamStore::do_write(&mut log, &mut streams, &mut categories, &mut stream_metadata, stream_name, m);
            next_pos = MessagePosition {
                revision: &next_pos.revision + 1,
                position: &next_pos.position + 1,
            }
        }

        append_result
    }
}

#[cfg(test)]
mod test {
    use super::MemoryStreamStore;
    use crate::store::{
        Message, ReadDirection, ReadFromCategory, ReadFromStream, StreamVersion, WriteResult,
        WriteToStream,
    };

    #[test]
    fn it_reads_events_forwards() {
        let mut store = MemoryStreamStore::new();
        let data_1 = r#"{"test": "data"}"#.as_bytes().to_vec();
        let msg_1 = Message {
            message_type: "TestMessage".to_owned(),
            data: data_1,
        };
        let data_2 = r#"{"test2": "data2"}"#.as_bytes().to_vec();
        let msg_2 = Message {
            message_type: "AnotherMessage".to_owned(),
            data: data_2,
        };

        let _ = store.write_to_stream("TestStream-1", StreamVersion::NoStream, &[msg_1, msg_2]);

        let (version, messages) = store.read_from_stream("TestStream-1", ReadDirection::Forwards);

        assert_eq!(version, StreamVersion::Revision(1));
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].message_type, "TestMessage");
        assert_eq!(messages[1].message_type, "AnotherMessage");
    }

    #[test]
    fn it_reads_a_category() {
        let mut store = MemoryStreamStore::new();

        let data_1 = r#"{"test": "data"}"#.as_bytes().to_vec();
        let msg_1 = Message {
            message_type: "TestMessage".to_owned(),
            data: data_1,
        };
        let data_2 = r#"{"test2": "data2"}"#.as_bytes().to_vec();
        let msg_2 = Message {
            message_type: "AnotherMessage".to_owned(),
            data: data_2,
        };

        store.write_to_stream("TestStream-1", StreamVersion::NoStream, &[msg_1, msg_2]);

        let data = r#"{"test3": "data3"}"#.as_bytes().to_vec();
        let msg = Message {
            message_type: "A third message".to_owned(),
            data: data.clone(),
        };
        store.write_to_stream("TestStream-2", StreamVersion::NoStream, &[msg]);

        let msg = Message {
            message_type: "A fourth message".to_owned(),
            data: data.clone(),
        };
        store.write_to_stream("DifferentCategory", StreamVersion::NoStream, &[msg]);

        let messages = store.read_from_category("TestStream", 0, None);
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0].message_type, "TestMessage");
        assert_eq!(messages[1].message_type, "AnotherMessage");
        assert_eq!(messages[2].message_type, "A third message");
    }

    #[test]
    fn it_reads_a_category_max_messages() {
        let mut store = MemoryStreamStore::new();

        let data = r#"{"test": "data"}"#.as_bytes().to_vec();
        let msg = Message {
            message_type: "TestMessage".to_owned(),
            data,
        };
        store.write_to_stream("TestStream-1", StreamVersion::NoStream, &[msg]);

        let data = r#"{"test3": "data3"}"#.as_bytes().to_vec();
        let msg = Message {
            message_type: "A second message".to_owned(),
            data: data.clone(),
        };
        store.write_to_stream("TestStream-2", StreamVersion::NoStream, &[msg]);

        let msg = Message {
            message_type: "A third message".to_owned(),
            data,
        };
        store.write_to_stream("TestStream-1", StreamVersion::Revision(0), &[msg]);

        let messages = store.read_from_category("TestStream", 0, Some(2));
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].message_type, "TestMessage");
        assert_eq!(messages[1].message_type, "A second message");
    }

    #[test]
    fn it_reads_a_category_from_offset() {
        let mut store = MemoryStreamStore::new();

        let data = r#"{"test": "data"}"#.as_bytes().to_vec();
        let msg = Message {
            message_type: "TestMessage".to_owned(),
            data,
        };
        store.write_to_stream("TestStream-1", StreamVersion::NoStream, &[msg]);

        let data = r#"{"test3": "data3"}"#.as_bytes().to_vec();
        let msg = Message {
            message_type: "A second message".to_owned(),
            data: data.clone(),
        };
        let global_position =
            match store.write_to_stream("TestStream-2", StreamVersion::NoStream, &[msg]) {
                WriteResult::Ok(position) => position.position,
                _ => unreachable!(),
            };

        let msg = Message {
            message_type: "A third message".to_owned(),
            data,
        };
        store.write_to_stream("TestStream-1", StreamVersion::Revision(0), &[msg]);

        let messages = store.read_from_category("TestStream", global_position, Some(2));
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].message_type, "A second message");
        assert_eq!(messages[1].message_type, "A third message");
    }

    #[test]
    fn it_reads_events_backwards() {
        let mut store = MemoryStreamStore::new();
        let data_1 = r#"{"test": "data"}"#.as_bytes().to_vec();
        let msg_1 = Message {
            message_type: "TestMessage".to_owned(),
            data: data_1,
        };
        let data_2 = r#"{"test2": "data2"}"#.as_bytes().to_vec();
        let msg_2 = Message {
            message_type: "AnotherMessage".to_owned(),
            data: data_2,
        };
        let _ = store.write_to_stream("TestStream-1", StreamVersion::NoStream, &[msg_1, msg_2]);

        let (version, messages) = store.read_from_stream("TestStream-1", ReadDirection::Backwards);

        assert_eq!(version, StreamVersion::Revision(1));
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].message_type, "AnotherMessage");
        assert_eq!(messages[1].message_type, "TestMessage");
    }

    #[test]
    fn it_handles_conflict() {
        let data = r#"{"test": "data"}"#.as_bytes().to_vec();
        let msg = Message {
            message_type: "TestMessage".to_owned(),
            data,
        };
        let mut store = MemoryStreamStore::new();
        store.write_to_stream("TestStream-1", StreamVersion::NoStream, &[msg.clone()]);

        let append_result = store.write_to_stream("TestStream-1", StreamVersion::NoStream, &[msg]);
        assert_eq!(append_result, WriteResult::WrongExpectedVersion);

        let (version, messages) = store.read_from_stream("TestStream-1", ReadDirection::Forwards);
        assert_eq!(messages.len(), 1);
        assert_eq!(version, StreamVersion::Revision(0));
    }
}
