/// A message that is writeable into a stream
#[derive(Clone)]
pub struct Message {
    /// The type of a message
    pub message_type: String,
    /// The data of a message
    pub data: Vec<u8>,
}

#[derive(Eq, PartialEq, Debug, Copy, Clone)]
pub struct MessagePosition {
    /// The global position of a message in the store. The first position should have an index of 0.
    pub position: usize,
    /// The position of a message within its stream. The first position should have an index of 0.
    pub revision: usize,
}

/// A message that is read from a stream
#[derive(Clone)]
pub struct StreamMessage {
    /// A unique identifier for a message.
    pub id: String,
    /// The type of a message.
    pub message_type: String,
    /// The data of a message.
    pub data: Vec<u8>,
    /// The positions (position and revision) of a message in the store and in its stream.
    pub position: MessagePosition,
}

/// The version of a stream
#[derive(Eq, PartialEq, Debug)]
pub enum StreamVersion {
    /// The stream does not exist.
    NoStream,
    /// The stream stream exists, and has the specified revision.
    Revision(usize),
}

/// The result of a write operation; will show the position of the last written message if
/// successfully written.
#[derive(Eq, PartialEq, Debug)]
pub enum WriteResult {
    /// The write was successful and the store/stream is now at the given position.
    Ok(MessagePosition),
    /// The write was unsuccessful because of an expected version mismatch.
    WrongExpectedVersion,
}

/// The direction in which to read a stream.
#[derive(Eq, PartialEq)]
pub enum ReadDirection {
    Forwards,
    Backwards,
}

/// A stream of messages, represented as a vector.
pub type Stream = Vec<StreamMessage>;

/// A trait that expresses the behaviour of reading from a stream
pub trait ReadFromStream {
    /// Read a given stream in its entirety.
    ///
    /// # Arguments
    /// * `stream_name` - The name of the stream to read.
    /// * `read_direction` - The direction in which to read the stream.
    fn read_from_stream(
        &self,
        stream_name: &str,
        read_direction: ReadDirection,
    ) -> (StreamVersion, Stream);
}

/// A trait that expresses the behaviour of writing to a stream
pub trait WriteToStream {
    /// Write events to a stream.
    ///
    /// # Arguments
    /// * `stream_name` - The stream to write to.
    /// * `expected_version` - The expected version of the stream at write time, used for Optimistic.
    /// Concurrency Control.
    /// * `messages` - A slice of messages to write into the stream.
    fn write_to_stream(
        &mut self,
        stream_name: &str,
        expected_version: StreamVersion,
        messages: &[Message],
    ) -> WriteResult;
}

/// A trait that expresses the behaviour of reading from a category.
///
/// Reading from a category will return all messages that are written to streams belonging to that
/// category, in-order.
///
/// The category for streams "Example-1" and "Example-2", for instance, would be
/// "Example". When reading the "Example" category, messages will be returned across these two
/// streams in the order in which they were inserted into their respective streams.
pub trait ReadFromCategory {
    /// Read messages from a category
    ///
    /// # Arguments
    /// * `category_name` - The name of the category to read.
    /// * `offset` - The offset at which to start reading. This refers to the global position of
    /// the store.
    /// * `max_messages` - The maximum number of messages to read. If None, all messages for the
    /// category will be returned.
    ///
    fn read_from_category(
        &mut self,
        category_name: &str,
        offset: usize,
        max_messages: Option<usize>,
    ) -> Stream;
}
