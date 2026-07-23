//! RPC invocation struct definitions.

use serde::{Serialize, Deserialize};
use crate::types::LogEntry;

/// Represents an AppendEntries RPC request.
#[derive(Debug, Serialize, Deserialize)]
pub struct AppendEntriesRequest<T> {
    /// The leader's term.
    pub term: u64,

    /// The ID of the leader so the follower can redirect clients.
    pub leader_id: u64,

    /// Index of the log entry immediately preceding new ones.
    pub prev_log_index: usize,

    /// Term of the prev_log_index entry.
    pub prev_log_term: u64,

    /// The entries to store. This is empty if the RPC is a heartbeat.
    pub entries: Vec<LogEntry<T>>,

    /// The leader's commit_index.
    pub leader_commit: usize
}

/// Represents an AppendEntries RPC response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    /// The current term of the server.
    pub term: u64,

    /// True if the request succeeded.
    /// 
    /// This happens if the follower contained an entry that matches 
    /// prev_log_index and prev_log_term
    pub success: bool
}

/// Represents a RequestVote RPC request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteRequest {
    /// The candidate's term.
    pub term: u64,

    /// The candidate's ID.
    pub candidate_id: u64,

    /// Index of the candidate's last log entry.
    pub last_log_index: u64,

    /// Term of the candidate's last log entry.
    pub last_log_term: u64
}

/// Represents a RequestVote RPC response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    /// The current_term of the responding server.
    pub term: u64,

    /// True if the vote was granted by the responding server.
    pub vote_granted: bool
}