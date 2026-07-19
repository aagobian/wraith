//! RPC invocation struct definitions.

use serde::{Serialize, Deserialize};
use crate::types::LogEntry;

/// Represents an AppendEntries RPC request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
    /// The leader's term.
    term: u64,

    /// The ID of the leader so the follower can redirect clients.
    leader_id: u64,

    /// Index of the log entry immediately preceding new ones.
    prev_log_index: u64,

    /// Term of the prev_log_index entry.
    prev_log_term: u64,

    /// The entry to store. This is empty if the RPC is a heartbeat.
    entry: Option<LogEntry>,

    /// The leader's commit_index.
    leader_commit: u64
}

/// Represents an AppendEntries RPC response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    /// The current term of the server.
    term: u64,

    /// True if the request succeeded.
    /// 
    /// This happens if the follower contained an entry that matches 
    /// prev_log_index and prev_log_term
    success: bool
}

/// Represents a RequestVote RPC request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteRequest {
    /// The candidate's term.
    term: u64,

    /// The candidate's ID.
    candidate_id: u64,

    /// Index of the candidate's last log entry.
    last_log_index: u64,

    /// Term of the candidate's last log entry.
    last_log_term: u64
}

/// Represents a RequestVote RPC response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    /// The current_term of the responding server.
    term: u64,

    /// True if the vote was granted by the responding server.
    vote_granted: bool
}