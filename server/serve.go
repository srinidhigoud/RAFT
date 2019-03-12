package main

import (
	"fmt"
	"log"
	rand "math/rand"
	"net"
	"time"
	context "golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/nyu-distributed-systems-fa18/lab-2-raft-srinidhigoud/pb"
)

// Messages that can be passed from the Raft RPC server to the main loop for AppendEntries
type AppendEntriesInput struct {
	arg      *pb.AppendEntriesArgs
	response chan pb.AppendEntriesRet
}

// Messages that can be passed from the Raft RPC server to the main loop for VoteInput
type VoteInput struct {
	arg      *pb.RequestVoteArgs
	response chan pb.RequestVoteRet
}

// Struct off of which we shall hang the Raft service
type Raft struct {
	AppendChan chan AppendEntriesInput
	VoteChan   chan VoteInput
}

func (r *Raft) AppendEntries(ctx context.Context, arg *pb.AppendEntriesArgs) (*pb.AppendEntriesRet, error) {
	c := make(chan pb.AppendEntriesRet)
	r.AppendChan <- AppendEntriesInput{arg: arg, response: c}
	result := <-c
	return &result, nil
}

func (r *Raft) RequestVote(ctx context.Context, arg *pb.RequestVoteArgs) (*pb.RequestVoteRet, error) {
	c := make(chan pb.RequestVoteRet)
	r.VoteChan <- VoteInput{arg: arg, response: c}
	result := <-c
	return &result, nil
}

// Compute a random duration in milliseconds
func randomDuration(r *rand.Rand, heartbeat bool) time.Duration {
	// Constant
	if heartbeat{
		const DurationMax = 5000
		const DurationMin = 1000
		return time.Duration(r.Intn(DurationMax-DurationMin)+DurationMin) * time.Millisecond
	} else {
		const DurationMax = 50000
		const DurationMin = 10000
		return time.Duration(r.Intn(DurationMax-DurationMin)+DurationMin) * time.Millisecond
	}
	
}


// Restart the supplied timer using a random timeout based on function above
func restartTimer(timer *time.Timer, r *rand.Rand, heartbeat bool) {
	stopped := timer.Stop()
	// If stopped is false that means someone stopped before us, which could be due to the timer going off before this,
	// in which case we just drain notifications.
	if !stopped {
		// Loop for any queued notifications
		for len(timer.C) > 0 {
			<-timer.C
		}

	}
	timer.Reset(randomDuration(r, heartbeat))
}



// Launch a GRPC service for this Raft peer.
func RunRaftServer(r *Raft, port int) {
	// Convert port to a string form
	portString := fmt.Sprintf(":%d", port)
	// Create socket that listens on the supplied port
	c, err := net.Listen("tcp", portString)
	if err != nil {
		// Note the use of Fatalf which will exit the program after reporting the error.
		log.Fatalf("Could not create listening socket %v", err)
	}
	// Create a new GRPC server
	s := grpc.NewServer()

	pb.RegisterRaftServer(s, r)
	log.Printf("Going to listen on port %v", port)

	// Start serving, this will block this function and only return when done.
	if err := s.Serve(c); err != nil {
		log.Fatalf("Failed to serve %v", err)
	}
}

func connectToPeer(peer string) (pb.RaftClient, error) {
	backoffConfig := grpc.DefaultBackoffConfig
	// Choose an aggressive backoff strategy here.
	backoffConfig.MaxDelay = 500 * time.Millisecond
	conn, err := grpc.Dial(peer, grpc.WithInsecure(), grpc.WithBackoffConfig(backoffConfig))
	// Ensure connection did not fail, which should not happen since this happens in the background
	if err != nil {
		return pb.NewRaftClient(nil), err
	}
	return pb.NewRaftClient(conn), nil
}




// The main service loop. All modifications to the KV store are run through here.
func serve(s *KVStore, r *rand.Rand, peers *arrayPeers, id string, port int) {
	
	const MaxUint = ^uint64(0) 
	const MinUint = 0 
	const MaxInt = int64(MaxUint >> 1) 
	const MinInt = -MaxInt - 1
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	raft := Raft{AppendChan: make(chan AppendEntriesInput), VoteChan: make(chan VoteInput)}
	// Start in a Go routine so it doesn't affect us.
	go RunRaftServer(&raft, port)
	peerClients := make(map[string]pb.RaftClient)
	//////////////////////////////////////////////////////////////STATE////////////////////////////////////////////////////////////
	
	total_peer_index := len(*peers)+1//here///////////////////////
	timer := time.NewTimer(randomDuration(r, false))
	CurrState := "1" //1-follower, 2-Candidate, 3-Leader
	votedFor := ""
	vote_count := 0
	localLeaderID := ""
	var local_log []*pb.Entry
	currentTerm := int64(0)
	localLastLogTerm := int64(0)
	localCommitIndex := int64(-1)
	localLastApplied := int64(-1)
	localLastLogIndex := int64(-1)
	
	
	
	localNextIndex := make(map[string]int64)
	localMatchIndex := make(map[string]int64)
	clientReq_id_map := make(map[int64]InputChannelType)

	
	for _, peer := range *peers {
		client, err := connectToPeer(peer)
		if err != nil {
			log.Fatalf("Failed to connect to GRPC server %v", err)
		}
		peerClients[peer] = client
		localNextIndex[peer] = 0
		localMatchIndex[peer] = -1
		log.Printf("Connected to %v", peer)
	}
	localMatchIndex[id] = -1
	localNextIndex[id] = 0

	type AppendResponse struct {
		ret  *pb.AppendEntriesRet
		err  error
		peer string
		len_ae int64 // length of append entries
		next_index int64 //old next index, helps removing redundant next index updates
	}

	type VoteResponse struct {
		ret  *pb.RequestVoteRet
		err  error
		peer string
	}
	appendResponseChan := make(chan AppendResponse)
	voteResponseChan := make(chan VoteResponse)

	


	// Run forever handling inputs from various channels
	for {
		
		select {
				
			case <-timer.C:
				// log.Printf("Timeout wentoff")
				if CurrState != "3" {
					log.Printf("Timeout %v", id)
					
					vote_count = 1
					fellow_peers := len(peerClients)///chekc here
					currentTerm += 1
					CurrState = "2"
					for p, c := range peerClients {
						// Send in parallel so we don't wait for each client.
						log.Printf("requesting from %v", p)
						go func(c pb.RaftClient, p string) {
							// ret, err := c.RequestVote(context.Background(), &pb.RequestVoteArgs{Term: 1, CandidateID: id})
							ret, err := c.RequestVote(context.Background(), &pb.RequestVoteArgs{Term: currentTerm, CandidateID: id, LastLogIndex: localLastLogIndex, LasLogTerm: localLastLogTerm})
							voteResponseChan <- VoteResponse{ret: ret, err: err, peer: p}
							// log.Printf("But now I entered timer time out thingy")
						}(c, p)
						// log.Printf("But now I exited timer time out thingy")
					}
					log.Printf("candidate %v requesting from %v peer", id, fellow_peers)
					restartTimer(timer, r, false)
				} else {
					// Send heartbeats
					// log.Printf("Sending heartbeats")
					new_entry_struct := pb.AppendEntriesArgs{Term: currentTerm, LeaderID: id, PrevLogIndex: localLastLogIndex, PrevLogTerm: localLastLogTerm, LeaderCommit: localCommitIndex}
					for p, c := range peerClients {
						go func(c pb.RaftClient, p string) {
							// log.Printf("Sending heartbeats to %v",p)
							ret, err := c.AppendEntries(context.Background(), &new_entry_struct)
							bufferNextIndex := localNextIndex[p]
							// _ = ret
							// _ = err
							appendResponseChan <- AppendResponse{ret: ret, err: err, peer: p, len_ae: int64(0), next_index: bufferNextIndex}
						}(c, p)
					}
					restartTimer(timer, r, true)
				}
				
			case op := <-s.C:
				log.Printf("We received an operation from a client")
				// We received an operation from a client
				// TODO: Figure out if you can actually handle the request here. If not use the Redirect result to send the
				// client elsewhere.
				// TODO: Use Raft to make sure it is safe to actually run the command.

				if CurrState == "3" {
					
					update := pb.Entry{Term: currentTerm, Index: localLastLogIndex + 1, Cmd: &op.command}
					local_log = append(local_log, &update)
					log.Printf("Leader received from a client")
					clientReq_id_map[update.Index] = op
					
					for p, c := range peerClients {
						log.Printf("Sending append entries to %v", p)
						go func(c pb.RaftClient, p string) {
							start := localNextIndex[p]
							local_prevLogTerm := int64(0)
							localPrevNextIndex := localNextIndex[p]
							log.Printf("length of local_log %v, localNextIndex[p] %v",len(local_log), start)
							if(start>0){
								local_prevLogTerm = local_log[start].Term
							}
							
							new_entry_list := local_log[start:]
							
							new_entry_struct := pb.AppendEntriesArgs{Term: currentTerm, LeaderID: id, PrevLogIndex: localNextIndex[p]-1, PrevLogTerm: local_prevLogTerm, LeaderCommit: localCommitIndex, Entries: new_entry_list}
							ret, err := c.AppendEntries(context.Background(), &new_entry_struct)
							appendResponseChan <- AppendResponse{ret: ret, err: err, peer: p, len_ae: int64(len(new_entry_list)), next_index: localPrevNextIndex}
						}(c, p)
					}
					localLastLogIndex += 1 
					localLastLogTerm = currentTerm
				} else {
					res := pb.Result{Result: &pb.Result_Redirect{Redirect: &pb.Redirect{Server: localLeaderID}}}
					op.response <- res
					log.Printf("Redirect to client")
				}

			case ae := <-raft.AppendChan:
				
				// We received an AppendEntries request from a Raft peer
				// TODO figure out what to do here, what we do is entirely wrong.
				leader_commit := ae.arg.LeaderCommit
				
				ae_list := ae.arg.Entries
				isHeartBeat := false
				if len(ae_list) == 0 {
					isHeartBeat = true  
				} 
				leader_PrevLogIndex := ae.arg.PrevLogIndex
				leader_PrevLogTerm := ae.arg.PrevLogTerm
				if isHeartBeat {
					// log.Printf("Received heartbeat from %v", localLeaderID)
					if ae.arg.Term > currentTerm {
						currentTerm = ae.arg.Term
						CurrState = "1"
						localLeaderID = ae.arg.LeaderID
						log.Printf("Term %v: Now the leader is %v in term %v (heartbeat)", currentTerm, localLeaderID)
					}
					if localLastLogIndex < leader_PrevLogIndex{
						log.Printf("failed because of heartbeat localLastLogIndex %v, leader_PrevLogIndex %v",localLastLogIndex,leader_PrevLogIndex)
						ae.response <- pb.AppendEntriesRet{Term: currentTerm, Success: false}
					}
					if localLastLogIndex < leader_commit {
						localCommitIndex = localLastLogIndex
					} else {
						localCommitIndex = leader_commit
					}
					// log.Printf("local commit index is %v, local leader's commit index is %v, leader's last log index is %v",localCommitIndex, leader_commit, leader_PrevLogIndex)
					// ae.response <- pb.AppendEntriesRet{Term: currentTerm, Success: true}
				} else {
					// log.Printf("Received append entry from %v", ae.arg.LeaderID)
					if ae.arg.Term < currentTerm {
						log.Printf("failed append entry as local term is bigger")
						ae.response <- pb.AppendEntriesRet{Term: currentTerm, Success: false}
					} else {
						log.Printf("localLastLogIndex %v leader_PrevLogIndex %v len(local_log) %v leader_PrevLogTerm %v length of append list %v", localLastLogIndex, leader_PrevLogIndex, len(local_log), leader_PrevLogTerm, len(ae_list))
						if localLastLogIndex < leader_PrevLogIndex {
							log.Printf("failed because leader has lengthier log : local last log index %v, leader prev log index %v",localLastLogIndex, leader_PrevLogIndex)
							ae.response <- pb.AppendEntriesRet{Term: currentTerm, Success: false}
						} else {
							if leader_PrevLogIndex != -1 && local_log[leader_PrevLogIndex].Term != leader_PrevLogTerm{
								log.Printf("Failed because terms are unequal")
								ae.response <- pb.AppendEntriesRet{Term: currentTerm, Success: false}
							} else {
								// deletion_stop := false
								if localLastLogIndex > leader_PrevLogIndex {
									log.Printf("checking because local log is lengthier")
									// ae.response <- pb.AppendEntriesRet{Term: currentTerm, Success: false}
									local_log = local_log[:(leader_PrevLogIndex+1)]
									for _, entry := range ae_list {
										local_log = append(local_log, entry)
										// if entry.Index > localLastLogIndex{
										// 	local_log = append(local_log, entry)
										// } else {
										// 	if local_log[entry.Index].Term != entry.Term{
										// 		local_log = local_log[:entry.Index]
										// 		local_log = append(local_log,entry)
										// 		localLastLogIndex = len(local_log)+1
										// 	}
										// }
									}
									log.Printf("Successfully edited the log")
									ae.response <- pb.AppendEntriesRet{Term: currentTerm, Success: true}
									
								} else {
									if localLastLogIndex == leader_PrevLogIndex {
										// log.Printf("Now appending entries into local log")
										for _, entry := range ae_list {
											local_log = append( local_log, entry)
										}
										log.Printf("Sucessfull in adding entire log")
										ae.response <- pb.AppendEntriesRet{Term: currentTerm, Success: true}
									} 
								}
								
								// localLastLogIndex = int64(len(local_log) - 1)
								
							}
						}
						currentTerm = ae.arg.Term // ?? here ??
						CurrState = "1" // ??
						localLeaderID = ae.arg.LeaderID // ?? here ??
						localLastLogIndex = int64(len(local_log) - 1)
						if len(local_log) > 0{
							localLastLogTerm = local_log[localLastLogIndex].Term
						} else {
							localLastLogTerm = -1
						}
						
						if leader_commit < localLastLogIndex {
							localCommitIndex = leader_commit
						} else {
							localCommitIndex = localLastLogIndex
						}
						// log.Printf("local commit index is %v, local leader's commit index is %v",localCommitIndex, leader_commit)
					}
				}
				// log.Printf("Done appending")
				restartTimer(timer, r, false)
			case vr := <-raft.VoteChan:
				// log.Printf("We received a RequestVote RPC from a raft peer")
				// We received a RequestVote RPC from a raft peer
				// TODO: Fix this.

				bufferTerm := vr.arg.Term
				bufferID := vr.arg.CandidateID
				bufferLastLogIndex := vr.arg.LastLogIndex
				bufferLasLogTerm := vr.arg.LasLogTerm
				// log.Printf("We received a RequestVote RPC and we are entering conditional check")
				if bufferTerm < currentTerm {
					vr.response <- pb.RequestVoteRet{Term: currentTerm, VoteGranted: false}
					// log.Printf("local term %v is bigger than candidate's term %v",currentTerm,bufferTerm)
				} else {
					if bufferTerm > currentTerm {
						// log.Printf("buffer's term is bigger")
						votedFor = ""
					}
					currentTerm = bufferTerm
					// log.Printf("We are entering second conditional check")
					if votedFor == "" || votedFor == bufferID {
						if bufferLasLogTerm > localLastLogTerm || bufferLastLogIndex >= localLastLogIndex {
							// log.Printf("Vote to be granted succesfully")
							vr.response <- pb.RequestVoteRet{Term: currentTerm, VoteGranted: true}
							votedFor = bufferID
							log.Printf("follower %v voted %v", id, votedFor)
							localLeaderID = votedFor
							// log.Printf("Vote granted succesfully")
						} else {
							// log.Printf("Vote grant failed 1")
							vr.response <- pb.RequestVoteRet{Term: currentTerm, VoteGranted: false}
						}
					} else {
						// log.Printf("Vote grant failed 2")
						vr.response <- pb.RequestVoteRet{Term: currentTerm, VoteGranted: false}
					}
				}
				
				// log.Printf("Exiting conditional check")
				CurrState = "1"
				restartTimer(timer, r, false) // ??

				
				// log.Printf("We received a RequestVote RPC from a raft peer")
				// vr.response <- pb.RequestVoteRet{Term: currentTerm, VoteGranted: false} // Should it be last call?
			case vr := <-voteResponseChan:
				// We received a response to a previou vote request.
				// log.Printf("We received a response to a previous vote request.")
				// TODO: Fix this
				if vr.err != nil {
					log.Printf("Error calling RPC %v", vr.err)
				} else {
					peer_term := vr.ret.Term
					// log.Printf("We entered no error and handling vote response at %v", id)
					check_vote_status := vr.ret.VoteGranted
					
					if peer_term > currentTerm {
						log.Printf("peer %v is now follower, peer term %v, current term %v", id, peer_term, currentTerm)
						CurrState = "1"
						currentTerm = peer_term
					} else {
						if check_vote_status {
							vote_count += 1
							if vote_count > total_peer_index/2 && CurrState == "2" {
								
								CurrState = "3"
								localLeaderID = id
								log.Printf("peer %v is now leader, peer term %v, %v / %v votes", id, peer_term, vote_count, total_peer_index)
								
								new_entry_struct := pb.AppendEntriesArgs{Term: currentTerm, LeaderID: id, PrevLogIndex: localLastLogIndex, PrevLogTerm: localLastLogTerm, LeaderCommit: localCommitIndex}
								for p, c := range peerClients {
									localNextIndex[p] = localLastLogIndex + 1
									go func(c pb.RaftClient, p string) {
										ret, err := c.AppendEntries(context.Background(), &new_entry_struct)
										bufferNextIndex := localNextIndex[p]
										// _ = ret
										// _ = err
										appendResponseChan <- AppendResponse{ret: ret, err: err, peer: p, len_ae: int64(0), next_index: bufferNextIndex}
									}(c, p)
								}
								restartTimer(timer, r, true)
							}
						}
					}	

				}
				// log.Printf("I am exiting response to a vote request")
			case ar := <-appendResponseChan:
				log.Printf("We received a response to a previous AppendEntries RPC call")
				// We received a response to a previous AppendEntries RPC call
				peer_index := ar.peer
				// followerTerm := ar.ret.Term
				lenOfAppendedEntries := ar.len_ae
				peer_prevNextIndex := ar.next_index
				if ar.err != nil {
					log.Printf("Error calling RPC %v", ar.err)
				} else {
					followerAppendSuccess := ar.ret.Success						
					if followerAppendSuccess {
						log.Printf("It was a successful append entry operation for %v",peer_index)
						
						
						if lenOfAppendedEntries > 0{
							buffer := int64(len(local_log))
							if localNextIndex[peer_index] < buffer{
								log.Printf("for peer %v: peer_prevNextIndex %v, localNextIndex[peer_index] %v, len(local_log) %v, localLastLogIndex %v, lenOfAppendedEntries %v",peer_prevNextIndex, peer_index, localNextIndex[peer_index],len(local_log), localLastLogIndex, lenOfAppendedEntries)
								localMatchIndex[peer_index] = peer_prevNextIndex + int64(lenOfAppendedEntries)-1


								localNextIndex[peer_index] = localMatchIndex[peer_index] + 1
								log.Printf("for peer %v: localNextIndex[peer_index] %v, len(local_log) %v, localLastLogIndex %v, lenOfAppendedEntries %v",peer_index, localNextIndex[peer_index],len(local_log), localLastLogIndex, lenOfAppendedEntries)
								

								log.Printf("Now checking commit indices")
								nextMaxlocalCommitIndex := localCommitIndex
								for i := localCommitIndex; i <= localLastLogIndex; i++ {
									peer_countReplicatedUptoi := 1
									for _, followerlocalMatchIndex := range localMatchIndex {
										if followerlocalMatchIndex >= i {
											peer_countReplicatedUptoi += 1
										}
									}
									if peer_countReplicatedUptoi > total_peer_index/2 {
										nextMaxlocalCommitIndex = i
									}
								}
								localCommitIndex = nextMaxlocalCommitIndex
							}
						}
						
						

					} else {
						if ar.ret.Term <= currentTerm{
							log.Printf("It was not a successful append entry operation")
						
							if localNextIndex[peer_index] <= localLastLogIndex && len(local_log) > 0 {
								if lenOfAppendedEntries>0 && localNextIndex[peer_index] > 0{
									log.Printf("Reducing peer %v next index %v",peer_index,localNextIndex[peer_index])
									localNextIndex[peer_index] = peer_prevNextIndex - 1
									log.Printf("Reducing peer %v next index %v",peer_index,localNextIndex[peer_index])
								}
								bufferNextIndex := localNextIndex[peer_index]
								bufferLastLogTerm := int64(0)
								if bufferNextIndex >=0 {
									bufferLastLogTerm = local_log[bufferNextIndex].Term
								}
								// log.Printf("2 %v,%v",bufferLastLogTerm,bufferNextIndex)
								bufferLastLogIndex := bufferNextIndex - 1
								replacingPlusNewEntries := local_log[bufferNextIndex:]
								
								buffernew_entry_struct := pb.AppendEntriesArgs{Term: currentTerm, LeaderID: id, PrevLogIndex: bufferLastLogIndex, PrevLogTerm: bufferLastLogTerm, LeaderCommit: localCommitIndex, Entries: replacingPlusNewEntries}
								log.Printf("It was not a successful append entry operation but successful call")
								go func(c pb.RaftClient, p string) {
									ret, err := c.AppendEntries(context.Background(), &buffernew_entry_struct)
									appendResponseChan <- AppendResponse{ret: ret, err: err, peer: p, len_ae: int64(len(replacingPlusNewEntries)), next_index: bufferNextIndex}
								}(peerClients[peer_index], peer_index)
							}
						}
						
					}
					
					log.Printf("Got append entries response from %v", ar.peer)	
				}
			default:
				//log.Printf("But this not that strange")
				if localCommitIndex > localLastApplied {
					localLastApplied += 1
					opCmd := local_log[localLastApplied].Cmd 
					clientRequest, existsInlocalMachine := clientReq_id_map[localLastApplied]
					if CurrState == "3" {
						if existsInlocalMachine {
							log.Printf("Handling command now")
							s.HandleCommandLeader(clientRequest) 
						}
					} else {
						s.HandleCommandFollower(opCmd)  
					}
					
				}
		}
	}
	log.Printf("Strange to arrive here")
}

