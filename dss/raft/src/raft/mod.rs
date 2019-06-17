use rand::Rng;
use std::sync::mpsc;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant};

use futures::sync::mpsc::UnboundedSender;
use futures::sync::oneshot;
use futures::Future;
use futures::Stream;
use labcodec;
use labrpc::RpcFuture;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
pub mod service;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use self::service::*;

pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

#[derive(PartialEq, Clone, Debug)]
pub enum Role {
    Follower,
    Leader,
    Candidate,
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    term: u64,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    role: Role,

    logs: Vec<LogEntry>,
    voted_for: Option<u64>,
    commit_index: usize,
    last_applied: usize,
    next_index: Vec<usize>,
    match_index: Vec<usize>,

    election_timeout: Instant,
    heartbeat_timeout: Instant,
    leader_id: Option<usize>,

    vote_count: u64,

    propose_sender: Sender<Propose>,
    propose_receiver: Receiver<Propose>,
    apply_ch: UnboundedSender<ApplyMsg>,
}

pub enum Propose {
    RequestVoteArgs(RequestVoteArgs, oneshot::Sender<RequestVoteReply>),
    RequestVoteReply(usize, Result<RequestVoteReply>),
    AppendEntriesArgs(AppendEntriesArgs, oneshot::Sender<AppendEntriesReply>),
    AppendEntriesReply(usize, (usize, Option<usize>), Result<AppendEntriesReply>),
    Message(Vec<u8>, oneshot::Sender<Result<(u64, u64)>>),
    Stop,
}

impl Stream for Raft {
    type Item = State;
    type Error = ();

    fn poll(&mut self) -> std::result::Result<futures::Async<Option<State>>, ()> {
        let state = match self.role {
            Role::Follower => self.step_follower(),
            Role::Candidate => self.step_candidate(),
            Role::Leader => self.step_leader(),
        };
        self.apply_commited();
        state
    }
}

impl Drop for Raft {
    fn drop(&mut self) {
        debug!("{} dropped", self.me);
    }
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();
        let (propose_sender, propose_receiver) = channel();
        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            term: 0,
            role: Role::Follower,
            logs: vec![LogEntry {
                term: 0,
                index: 0,
                log: Vec::new(),
            }],
            voted_for: Option::None,
            commit_index: 0,
            last_applied: 0,
            next_index: Vec::new(),
            match_index: Vec::new(),

            election_timeout: Instant::now()
                + Duration::from_millis(rand::thread_rng().gen_range(0, 150)),
            heartbeat_timeout: Instant::now(),
            leader_id: Option::None,
            vote_count: 0,

            propose_sender,
            propose_receiver,
            apply_ch,
        };
        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/mod.rs for more details.
    fn send_request_vote(&self, server: usize, args: &RequestVoteArgs) {
        let me = self.me;
        let peer = &self.peers[server];
        let propose_sender = self.propose_sender.clone();
        peer.spawn(
            peer.request_vote(args)
                .map_err(Error::Rpc)
                .then(move |res| {
                    propose_sender
                        .send(Propose::RequestVoteReply(server, res))
                        .map(|res| ())
                        .map_err(|err| {
                            debug!("{} error sending vote_rep to {}: {:?}", me, server, err);
                        })
                }),
        );
    }

    fn send_append_entries(&self, server: usize, args: &AppendEntriesArgs) {
        let me = self.me;
        let peer = &self.peers[server];
        let propose_sender = self.propose_sender.clone();
        let (prev_log_index, last_log_index) = (
            args.prev_log_index as usize,
            args.entries.last().map(|last| last.index as usize),
        );
        peer.spawn(
            peer.append_entries(args)
                .map_err(Error::Rpc)
                .then(move |res| {
                    propose_sender
                        .send(Propose::AppendEntriesReply(
                            server,
                            (prev_log_index, last_log_index),
                            res,
                        ))
                        .map(|res| ())
                        .map_err(|err| {
                            debug!("{} error sending app_rep to {}: {:?}", me, server, err);
                        })
                }),
        );
    }

    fn last_log(&self) -> (u64, u64) {
        self.logs
            .last()
            .map(|last| (last.index, last.term))
            .unwrap_or((0, 0))
    }

    fn start(&mut self, log: Vec<u8>) -> Result<(u64, u64)> {
        let term = self.term;
        let (prev_log_index, prev_log_term) = self.last_log();
        let index = self.logs.len();
        let log_entry = LogEntry {
            term,
            log,
            index: index as u64,
        };
        info!(
            "({}, {}) start {} : ({}, {})",
            self.me, self.term, index, prev_log_index, prev_log_term
        );
        self.logs.push(log_entry.clone());
        let args = AppendEntriesArgs {
            term: self.term,
            leader_id: self.me as u64,
            prev_log_index,
            prev_log_term,
            entries: vec![log_entry],
            leader_commit: self.commit_index as u64,
        };
        self.match_index[self.me] = index;
        self.next_index[self.me] = index + 1;
        for peer in 0..self.peers.len() {
            if peer != self.me {
                self.send_append_entries(peer, &args);
            }
        }
        Ok((index as u64, term))
    }

    fn reset_election_timeout(&mut self) {
        self.election_timeout =
            Instant::now() + Duration::from_millis(rand::thread_rng().gen_range(200, 400));
    }

    fn check_campagin(&mut self) {
        if self.election_timeout < Instant::now() {
            self.role = Role::Candidate;
            self.vote_count = 1;
            self.term += 1;
            self.voted_for = Some(self.me as u64);
            info!("({}, {}) starts election", self.me, self.term);
            self.reset_election_timeout();
            self.campagin();
        }
    }

    fn campagin(&self) {
        let (last_log_index, last_log_term) = self.last_log();
        let args = RequestVoteArgs {
            term: self.term,
            candidate_id: self.me as u64,
            last_log_index: last_log_index as u64,
            last_log_term: last_log_term as u64,
        };
        for peer in 0..self.peers.len() {
            if peer != self.me {
                self.send_request_vote(peer, &args);
            }
        }
    }

    fn check_heartbeat(&mut self) {
        if self.heartbeat_timeout < Instant::now() {
            self.heartbeat_timeout = Instant::now() + Duration::from_millis(150);
            self.heartbeat();
        }
    }

    fn heartbeat(&self) {
        debug!("({}, {}) starts heartbeat", self.me, self.term);
        let (prev_log_index, prev_log_term) = self.last_log();
        let args = AppendEntriesArgs {
            term: self.term,
            leader_id: self.me as u64,
            prev_log_index,
            prev_log_term,
            entries: Vec::new(),
            leader_commit: self.commit_index as u64,
        };
        for peer in 0..self.peers.len() {
            if peer != self.me {
                self.send_append_entries(peer, &args);
            }
        }
    }

    fn state(&self) -> std::result::Result<futures::Async<Option<State>>, ()> {
        Ok(futures::Async::Ready(Some(State {
            term: self.term,
            is_leader: self.role == Role::Leader,
        })))
    }

    fn apply_commited(&mut self) {
        loop {
            if self.last_applied < self.commit_index {
                let command_index = self.last_applied + 1;
                let next = self.logs.get(command_index).unwrap();
                assert!(next.index == command_index as u64);
                info!("({}, {}) applied ({}, {})", self.me, self.term, command_index, next.term);
                self.apply_ch
                    .unbounded_send(ApplyMsg {
                        command_valid: true,
                        command: next.log.clone(),
                        command_index: command_index as u64,
                    })
                    .unwrap();
                self.last_applied = command_index;
            } else {
                break;
            }
        }
    }

    fn step_append(
        &mut self,
        mut app_ents: AppendEntriesArgs,
        rep_sender: oneshot::Sender<AppendEntriesReply>,
    ) -> std::result::Result<futures::Async<Option<State>>, ()> {
        debug!(
            "({}, {}) receives append request from ({}, {})",
            self.me, self.term, app_ents.leader_id, app_ents.term
        );
        if app_ents.term >= self.term {
            self.reset_election_timeout();
            self.role = Role::Follower;
            self.leader_id = Some(app_ents.leader_id as usize);
            self.term = app_ents.term;
            if let Some(true) = self
                .logs
                .get(app_ents.prev_log_index as usize)
                .map(|log| log.term == app_ents.prev_log_term)
            {
                self.logs.truncate(app_ents.prev_log_index as usize + 1);
                if !app_ents.entries.is_empty() {
                    info!(
                        "({}, {}) append {} -> {}",
                        self.me,
                        self.term,
                        self.logs.last().map(|last| last.index).unwrap_or(0),
                        app_ents.entries.last().unwrap().index
                    );
                }
                self.logs.append(&mut app_ents.entries);

                if app_ents.leader_commit > self.commit_index as u64 {
                    let to_commit = std::cmp::min(
                        self.logs.last().map(|last| last.index).unwrap_or(0),
                        app_ents.leader_commit,
                    );
                    info!(
                        "({}, {}) commit {} -> {}",
                        self.me, self.term, self.commit_index, to_commit
                    );
                    self.commit_index = to_commit as usize;
                }
                rep_sender
                    .send(AppendEntriesReply {
                        term: self.term,
                        success: true,
                    })
                    .unwrap();
            } else {
                info!(
                    "({}, {}) log mismatch at {}",
                    self.me, self.term, app_ents.prev_log_index,
                );
                rep_sender
                    .send(AppendEntriesReply {
                        term: self.term,
                        success: false,
                    })
                    .unwrap();
            }
        } else {
            rep_sender
                .send(AppendEntriesReply {
                    term: self.term,
                    success: false,
                })
                .unwrap();
        }
        self.state()
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.next_index = vec![self.logs.len(); self.peers.len()];
        self.match_index = vec![0; self.peers.len()];
    }

    fn step_follower(&mut self) -> std::result::Result<futures::Async<Option<State>>, ()> {
        match self
            .propose_receiver
            .recv_timeout(Duration::from_millis(10))
        {
            Ok(Propose::RequestVoteArgs(req_vote, rep_sender)) => {
                debug!(
                    "({}, {}) receives vote request from ({}, {})",
                    self.me, self.term, req_vote.candidate_id, req_vote.term
                );
                if self.term <= req_vote.term {
                    if self.term < req_vote.term {
                        self.term = req_vote.term;
                        self.voted_for = None;
                    }

                    let (last_log_index, last_log_term) = self.last_log();
                    if (req_vote.last_log_term > last_log_term)
                        || (req_vote.last_log_term == last_log_term
                            && req_vote.last_log_index >= last_log_index)
                    {
                        match self.voted_for {
                            Some(others) if others != req_vote.candidate_id => {}
                            _ => {
                                self.voted_for = Some(req_vote.candidate_id);
                                self.leader_id = None;
                                rep_sender
                                    .send(RequestVoteReply {
                                        current_term: self.term,
                                        vote_granted: true,
                                    })
                                    .unwrap();
                                info!(
                                    "({}, {}) granted vote to ({}, {})",
                                    self.me, self.term, req_vote.candidate_id, req_vote.term
                                );
                                self.check_campagin();
                                return self.state();
                            }
                        }
                    }
                }
                rep_sender
                    .send(RequestVoteReply {
                        current_term: self.term,
                        vote_granted: false,
                    })
                    .unwrap();
                info!(
                    "({}, {}) denied vote to ({}, {})",
                    self.me, self.term, req_vote.candidate_id, req_vote.term
                );

                self.check_campagin();
                self.state()
            }
            Ok(Propose::AppendEntriesArgs(app_ents, rep_sender)) => {
                self.step_append(app_ents, rep_sender)
            }
            Ok(Propose::Message(_, rep_sender)) => {
                rep_sender.send(Err(Error::NotLeader)).unwrap();
                self.state()
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                self.check_campagin();
                self.state()
            }
            Ok(Propose::Stop) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                Ok(futures::Async::Ready(None))
            }
            _ => {
                self.check_campagin();
                self.state()
            }
        }
    }

    fn step_candidate(&mut self) -> std::result::Result<futures::Async<Option<State>>, ()> {
        match self
            .propose_receiver
            .recv_timeout(Duration::from_millis(10))
        {
            Ok(Propose::RequestVoteArgs(req_vote, rep_sender)) => {
                rep_sender
                    .send(RequestVoteReply {
                        current_term: self.term,
                        vote_granted: false,
                    })
                    .unwrap();
                self.state()
            }
            Ok(Propose::RequestVoteReply(server, Ok(rep_vote))) => {
                if rep_vote.current_term == self.term && rep_vote.vote_granted {
                    self.vote_count += 1;
                }
                if rep_vote.current_term != self.term {
                    self.role = Role::Follower;
                }
                if self.vote_count > (self.peers.len() / 2) as u64 {
                    info!("({}, {}) win the election", self.me, self.term);
                    self.become_leader();
                    self.heartbeat();
                }
                self.state()
            }
            Ok(Propose::AppendEntriesArgs(app_ents, rep_sender)) => {
                self.step_append(app_ents, rep_sender)
            }
            Ok(Propose::Message(_, rep_sender)) => {
                rep_sender.send(Err(Error::NotLeader)).unwrap();
                self.state()
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                self.check_campagin();
                self.state()
            }
            Ok(Propose::Stop) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                Ok(futures::Async::Ready(None))
            }
            _ => {
                self.check_campagin();
                self.state()
            }
        }
    }

    fn step_leader(&mut self) -> std::result::Result<futures::Async<Option<State>>, ()> {
        self.reset_election_timeout();
        match self
            .propose_receiver
            .recv_timeout(Duration::from_millis(10))
        {
            Ok(Propose::RequestVoteArgs(req_vote, rep_sender)) => {
                if req_vote.term <= self.term {
                    rep_sender
                        .send(RequestVoteReply {
                            current_term: self.term,
                            vote_granted: false,
                        })
                        .unwrap();
                } else {
                    self.term = req_vote.term;
                    self.role = Role::Follower;
                    rep_sender
                        .send(RequestVoteReply {
                            current_term: self.term,
                            vote_granted: true,
                        })
                        .unwrap();
                }
                self.state()
            }
            Ok(Propose::AppendEntriesArgs(app_ents, rep_sender)) => {
                if app_ents.term == self.term {
                    panic!(
                        "Two leader for term {}: {} and {}",
                        self.term, app_ents.leader_id, self.me
                    );
                }
                self.state()
            }
            Ok(Propose::AppendEntriesReply(
                server,
                (prev_log_index, last_log_index),
                Ok(rep_append),
            )) => {
                if rep_append.term > self.term {
                    self.role = Role::Follower
                } else if rep_append.success {
                    if let Some(last_log_index) = last_log_index {
                        let current = self.match_index[server];
                        let new = std::cmp::max(current, last_log_index);
                        self.match_index[server] = new;
                        self.next_index[server] = new + 1;
                        self.try_commit(last_log_index);
                    }
                } else {
                    self.next_index[server] = prev_log_index;
                    let logs = &self.logs[prev_log_index..self.logs.len()];
                    assert!(prev_log_index > 0);
                    let prev_log_index = prev_log_index - 1;
                    let prev_log_term = self.logs.get(prev_log_index).unwrap().term;

                    self.send_append_entries(
                        server,
                        &AppendEntriesArgs {
                            term: self.term,
                            leader_id: self.me as u64,
                            prev_log_index: prev_log_index as u64,
                            prev_log_term: prev_log_term as u64,
                            entries: logs.to_vec(),
                            leader_commit: self.commit_index as u64,
                        },
                    )
                }
                self.state()
            }
            Ok(Propose::Message(msg, rep_sender)) => {
                rep_sender.send(self.start(msg)).unwrap();
                self.state()
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                self.check_heartbeat();
                self.state()
            }
            Ok(Propose::Stop) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                Ok(futures::Async::Ready(None))
            }
            _ => {
                self.check_heartbeat();
                self.state()
            }
        }
    }

    fn try_commit(&mut self, index: usize) {
        if self.logs.get(index).unwrap().term == self.term && index > self.commit_index {
            let match_count: usize = (0..self.peers.len())
                .map(|server| {
                    if *self.match_index.get(server).unwrap() >= index {
                        1usize
                    } else {
                        0usize
                    }
                })
                .sum();
            if match_count > self.peers.len() / 2 {
                self.commit_index = index;
                info!(
                    "({}, {}) commited {}: {}",
                    self.me, self.term, index, match_count
                );
            } else {
                debug!(
                    "({}, {}) cannot commit {}: {}",
                    self.me, self.term, index, match_count
                );
            }
        }
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    propose_sender: Sender<Propose>,
    state: std::sync::Arc<std::sync::RwLock<State>>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        let propose_sender = raft.propose_sender.clone();
        let state = std::sync::Arc::new(std::sync::RwLock::new(State {
            term: raft.term,
            is_leader: raft.role == Role::Leader,
        }));
        let s = state.clone();
        let me = raft.me;
        thread::spawn(move || {
            let mut raft = raft;
            loop {
                match raft.poll() {
                    Ok(futures::Async::Ready(Some(state))) => {
                        let mut s = s.write().unwrap();
                        s.term = state.term;
                        s.is_leader = state.is_leader;
                    }
                    Ok(futures::Async::Ready(None)) => {
                        break;
                    }
                    Ok(futures::Async::NotReady) => {}
                    Err(err) => {
                        panic!("{:?}", err);
                    }
                }
            }
            debug!("{} quited", me);
        });
        Node {
            propose_sender,
            state,
        }
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let (tx, rx) = oneshot::channel();
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        self.propose_sender.send(Propose::Message(buf, tx)).unwrap();
        rx.wait().unwrap()
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.get_state().term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.get_state().is_leader
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        self.state.read().unwrap().clone()
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        self.propose_sender.send(Propose::Stop).unwrap();
        // Your code here, if desired.
    }
}

impl RaftService for Node {
    // example RequestVote RPC handler.
    fn request_vote(&self, args: RequestVoteArgs) -> RpcFuture<RequestVoteReply> {
        let (tx, rx) = oneshot::channel();
        self.propose_sender
            .send(Propose::RequestVoteArgs(args, tx))
            .unwrap();
        Box::new(rx.map_err(|c| labrpc::Error::Timeout))
    }

    fn append_entries(&self, args: AppendEntriesArgs) -> RpcFuture<AppendEntriesReply> {
        let (tx, rx) = oneshot::channel();
        self.propose_sender
            .send(Propose::AppendEntriesArgs(args, tx))
            .unwrap();
        Box::new(rx.map_err(|c| labrpc::Error::Timeout))
    }
}
