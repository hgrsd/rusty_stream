#[derive(Clone)]
pub struct Message {
    pub message_type: String,
    pub data: Vec<u8>,
}

#[derive(Clone)]
pub struct StreamMessage {
    pub id: String,
    pub message_type: String,
    pub data: Vec<u8>,
    pub revision: u64,
}

#[derive(Eq, PartialEq, Debug)]
pub enum StreamVersion {
    NoStream,
    Revision(u64),
}

#[derive(Eq, PartialEq, Debug)]
pub enum WriteResult {
    Ok(StreamVersion),
    WrongExpectedVersion,
}

pub type Stream = Vec<StreamMessage>;
pub trait ReadStream {
    fn read_stream(&self, stream_name: &str, max: Option<usize>) -> (StreamVersion, Stream);
}

pub trait WriteToStream {
    fn write_to_stream(&mut self, stream_name: &str, expected_version: StreamVersion, message: Message) -> WriteResult;
}
