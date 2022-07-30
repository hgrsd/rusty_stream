#[derive(Clone)]
pub struct Message {
    pub message_type: String,
    pub data: Vec<u8>,
}

#[derive(Eq, PartialEq, Debug, Clone)]
pub struct MessagePosition {
    pub position: usize,
    pub revision: usize,
}

#[derive(Clone)]
pub struct StreamMessage {
    pub id: String,
    pub message_type: String,
    pub data: Vec<u8>,
    pub position: MessagePosition,
}

#[derive(Eq, PartialEq, Debug)]
pub enum StreamVersion {
    NoStream,
    Revision(usize),
}

#[derive(Eq, PartialEq, Debug)]
pub enum WriteResult {
    Ok(MessagePosition),
    WrongExpectedVersion,
}

#[derive(Eq, PartialEq)]
pub enum ReadDirection {
    Forwards,
    Backwards,
}

pub type Stream = Vec<StreamMessage>;

pub trait ReadFromStream {
    fn read_from_stream(
        &self,
        stream_name: &str,
        read_direction: ReadDirection,
    ) -> (StreamVersion, Stream);
}

pub trait WriteToStream {
    fn write_to_stream(
        &mut self,
        stream_name: &str,
        expected_version: StreamVersion,
        message: Message,
    ) -> WriteResult;
}

pub trait ReadFromCategory {
    fn read_from_category(
        &mut self,
        category_name: &str,
        offset: usize,
        max_messages: Option<usize>,
    ) -> Stream;
}
