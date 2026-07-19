//! Raft core functionality.
//! 
//! This module defines the central RaftState struct and all functions 
//! necessary for the Raft consensus algorithm.

use std::collections::HashMap;
use crate::types::{NodeRole, LogEntry};

/// Represent's a server's state.
pub struct RaftState {    
    /// The latest term that the server has seen.
    current_term: u64,

    /// The ID of the candidate that the server last voted for. None if no vote occurred.
    voted_for: Option<u64>,

    /// A list of log entries.
    log: Vec<LogEntry>,


    /// The index of the highest entry committed.
    commit_index: u64,

    /// The index of the highest entry actually applied to state.
    last_applied: u64,


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

impl RaftState {
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
            log: Vec::new(),
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

            self.next_index.insert(i, self.commit_index + 1);
            self.match_index.insert(i, 0);
        }
    }

    /// Transitions a node to a follower.
    pub fn become_follower(&mut self) {
        self.role = NodeRole::Follower;
    }

    /// Transitions a node to a candidate. Sends RequestVote RPCs to all other nodes.
    pub fn become_candidate(&mut self) {
        self.role = NodeRole::Candidate;
        self.voted_for = Some(self.id);

        todo!("Invoke RequestVote RPCs here");
    }

    /// Progresses time by 1 tick. 
    /// Triggers an election timeout if the time elapsed exceeds the election timeout limit.
    pub fn tick(&mut self) {
        self.time_elapsed += 1;

        if self.time_elapsed >= self.election_timeout {
            self.become_candidate();
        }  
    }
}