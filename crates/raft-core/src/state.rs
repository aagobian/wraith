//! Raft core functionality.
//! 
//! This module defines the central RaftState struct and all functions 
//! necessary for the Raft consensus algorithm.

use std::collections::HashMap;
use std::todo;
use crate::types::{NodeRole, LogEntry};
use crate::rpc::{AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse};

/// Represent's a server's state.
pub struct RaftState<T> {    
    /// The latest term that the server has seen.
    current_term: u64,

    /// The ID of the candidate that the server last voted for. None if no vote occurred.
    voted_for: Option<u64>,

    /// A list of log entries.
    log: Vec<LogEntry<T>>,


    /// The index of the highest entry committed.
    commit_index: usize,

    /// The index of the highest entry actually applied to state.
    last_applied: usize,


    /// Leader: A HashMap containing the index of the next log entry to send to a given server.
    next_index: HashMap<u64, u64>,

    /// Leader: A HashMap containing the index of the the highest log entry replicated on a given server.
    match_index: HashMap<u64, u64>,

    /// The ID of the server.
    id: u64,

    /// The current role of the server.
    role: NodeRole,

    /// The time elapsed since the last heartbeat.
    time_elapsed: u64,

    /// The time limit that defines when the leader is considered unresponsive and the node starts an election. 
    election_timeout: u64,

    /// Leader: The interval at which a heartbeat should be sent.
    heartbeat: u64,
}

impl <T: Default> RaftState<T> {
    /// Creates and returns a new RaftState struct.
    pub fn new(node_id: u64) -> Self {
        RaftState {
            role: NodeRole::Follower,
            time_elapsed: 0,
            election_timeout: rand::random_range(15..31),
            heartbeat: 3,

            id: node_id,
            current_term: 1,
            voted_for: None,
            log: vec![ LogEntry { index: 0, term: 0, payload: T::default().into() } ],
            commit_index: 0,
            last_applied: 0,

            next_index: HashMap::new(),
            match_index: HashMap::new(),
        }
    }

    /// Transitions a node to the leader of the algorithm. Reinitializes proper fields.
    pub fn become_leader(&mut self) {
        self.role = NodeRole::Leader;

        self.next_index = HashMap::new();
        self.match_index = HashMap::new();
        self.current_term += 1;

        for i in 1..5 {
            if i == self.id {
                continue;
            }

            self.next_index.insert(i, self.log.len() as u64);
            self.match_index.insert(i, 0);
        }
    }

    /// Transitions a node to a follower.
    pub fn become_follower(&mut self) {
        self.role = NodeRole::Follower;
    }

    /// Transitions a node to a candidate. Sends RequestVote RPCs to all other nodes.
    pub fn become_candidate(&mut self) {
        self.current_term += 1;
        self.role = NodeRole::Candidate;
        self.voted_for = Some(self.id);

        todo!("Issue RequestVote RPCs here?")
    }

    /// Unpacks a given payload Option<T>, then creates and returns an AppendEntries RPC Request.
    pub fn create_append_entries(&self, recipient_id: u64) -> Result<AppendEntriesRequest<T>, &str> {
        let index = match self.next_index.get(&recipient_id) {
            Some(i) => *i as usize,
            None => { return Err("Uninitialized next_index for {recipient_id}") }
        };

        // Returns empty vec if index out of bounds (for heartbeat)
        let entries = self.log.get(index..)
            .map(|slice| slice.to_vec())
            .unwrap_or_default();

        Ok(AppendEntriesRequest {
            term: self.current_term,    
            leader_id: self.id,
            prev_log_index: (self.log.len()).saturating_sub(1),
            prev_log_term: self.log.last().map_or(0, |e| e.term),
            entries: entries,
            leader_commit: self.commit_index
        })
    }

    /// Handles an incoming AppendEntries request. 
    /// 
    /// It does this by:
    ///     - Validating the request's term is not higher than the follower term.
    ///     - Setting follower state (heartbeat timer, term, role)
    ///     - Validating prev_log_index is in bounds and the entry there matches prev_log_term.
    ///     - Finding a potential point of conflict and truncating it.
    ///     - Appending the entries to the log. 
    ///     - Updating commit_index if less than the leader's commit_index.
    pub fn handle_append_entries(&mut self, req: AppendEntriesRequest<T>) -> AppendEntriesResponse {        
        if req.term < self.current_term {
            return AppendEntriesResponse { 
                term: self.current_term, 
                success: false
            }
        }

        self.current_term = req.term;
        self.role = NodeRole::Follower;
        self.voted_for = None;
        self.time_elapsed = 0;

        match self.log.get(req.prev_log_index) {
            Some(entry) if entry.term == req.prev_log_term => {},
            _ => { 
                return AppendEntriesResponse { 
                    term: self.current_term, 
                    success: false
                }
            }
        }

        let mut log_index = req.prev_log_index + 1;
        let mut entry_index = 0;

        while log_index < self.log.len() && entry_index < req.entries.len() {
            if self.log[log_index].term != req.entries[entry_index].term {
                self.log.truncate(log_index);
                break;
            }
            log_index += 1;
            entry_index += 1;
        }

        self.log.extend_from_slice(&req.entries[entry_index..]);
        
        if self.commit_index < req.leader_commit {
            self.commit_index = std::cmp::min(req.leader_commit, self.log.len() - 1);
        }

        // TODO: Evaluate whether persistence is needed/should be here.

        AppendEntriesResponse { 
            term: self.current_term, 
            success: true
        }
    }

    /// Creates and returns a RequestVote RPC request.
    pub fn create_request_vote(&self) -> RequestVoteRequest {
        RequestVoteRequest {
            term: self.current_term,
            candidate_id: self.id,
            last_log_index: self.log.len() as u64,
            last_log_term: self.log.last().map_or(0, |e| e.term)
        }
    }

    pub fn handle_request_vote(&mut self) {

    }

    /// Progresses time by 1 tick. 
    /// Triggers an election timeout if the time elapsed exceeds the election timeout limit.
    pub fn tick(&mut self) {
        self.time_elapsed += 1;
        
        match self.role {
            NodeRole::Candidate | NodeRole::Follower => {
                if self.time_elapsed >= self.election_timeout {
                    self.become_candidate();
                }  
            }
            NodeRole::Leader => {
                if self.time_elapsed >= self.heartbeat {
                    // self.broadcast_heartbeats(); TODO: Implement this somewhere
                    self.time_elapsed = 0;
                }
            }
        }
    }
}