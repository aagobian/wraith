//! Type definitions essential for Raft functionality.

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
/// Contains the term the entry was logged in and an opaque pointer to its payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry<T> {
    /// The term this entry exists in.
    pub term: u64,

    /// The content of the log.
    pub payload: T
}

