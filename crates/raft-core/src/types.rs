//! Type definitions essential for Raft functionality.

use std::sync::Arc;
use serde::{Serialize, Deserialize};

/// Denotes the role status of a node. A node may be either a Leader, Follower, or Candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    Leader,
    Follower,
    Candidate
}

/// Represents an entry in a server's log.
/// 
/// Contains the index and term the entry was logged in along with an opaque pointer to its payload.
#[derive(Debug, Serialize, Deserialize)]
pub struct LogEntry<T> {
    /// The index of this log entry.
    pub index: u64,

    /// The term this entry exists in.
    pub term: u64,

    /// The content of the log. Wrapping T in Arc allows for cheap clones. 
    /// Note that this does not preserve shared refs on deserialization.
    pub payload: Arc<T>
}

impl<T> Clone for LogEntry<T> {
    fn clone(&self) -> Self {
        LogEntry { 
            index: self.index, 
            term: self.term, 
            payload: Arc::clone(&self.payload), 
        }
    }

}

