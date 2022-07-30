use std::collections::HashMap;
use std::sync::RwLock;

use uuid::Uuid;

use crate::stream::{
    Message, MessagePosition, ReadDirection, ReadFromCategory, ReadFromStream, Stream,
    StreamMessage, StreamVersion, WriteResult, WriteToStream,
};

type Index = HashMap<String, Vec<usize>>;

struct MemoryStreamStore {
    log: RwLock<Vec<StreamMessage>>,
    streams: RwLock<Index>,
    stream_metadata: RwLock<HashMap<String, usize>>,
    categories: RwLock<Index>,
}

impl MemoryStreamStore {
    pub fn new() -> Self {
        Self {
            log: RwLock::new(Vec::new()),
            streams: RwLock::new(HashMap::new()),
            stream_metadata: RwLock::new(HashMap::new()),
            categories: RwLock::new(HashMap::new()),
        }
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
        let pointers = index.get(stream_name);
        let mut version = StreamVersion::NoStream;
        let messages = match pointers {
            None => Vec::new(),
            Some(s) => {
                let mut v = Vec::with_capacity(s.len());
                for pointer in s {
                    let message = log.get(*pointer).unwrap();
                    version = StreamVersion::Revision(message.position.revision);
                    v.push(message.clone());
                }
                v
            }
        };

        let iter = messages.into_iter();
        let stream = if direction == ReadDirection::Forwards {
            iter.collect()
        } else {
            iter.rev().collect()
        };

        (version, stream)
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
        match index.get(category_name) {
            None => vec![],
            Some(s) => {
                let start = if offset == 0 {
                    0
                } else {
                    match s.binary_search(&offset) {
                        Ok(x) => x,
                        Err(x) => x,
                    }
                };
                let mut v = Vec::with_capacity(s.len());
                for pointer in s[start..].iter().take(max_messages.unwrap_or(s.len())) {
                    v.push(log.get(*pointer).unwrap().clone());
                }
                v
            }
        }
    }
}

impl WriteToStream for MemoryStreamStore {
    fn write_to_stream(
        &mut self,
        stream_name: &str,
        expected_version: StreamVersion,
        message: Message,
    ) -> WriteResult {
        let mut log = self.log.write().unwrap();
        let mut streams = self.streams.write().unwrap();
        let mut categories = self.categories.write().unwrap();
        let mut stream_metadata = self.stream_metadata.write().unwrap();

        let pointers = streams.get_mut(stream_name);

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

        log.push(m);

        if let Some(existing_stream) = pointers {
            existing_stream.push(new_position.position);
        } else {
            streams.insert(stream_name.to_owned(), vec![new_position.position]);
        };

        let category = stream_name
            .split('-')
            .nth(0)
            .expect("No category can be inferred from stream");
        if let Some(existing_category) = categories.get_mut(category) {
            existing_category.push(new_position.position);
        } else {
            categories.insert(category.to_owned(), vec![new_position.position]);
        };

        stream_metadata.insert(stream_name.to_owned(), new_position.revision);
        WriteResult::Ok(new_position)
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
