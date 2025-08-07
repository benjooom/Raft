package raft

import (
	"fmt"
	"testing"
	"time"
)

func TestALotOfServersSimple(t *testing.T) {
	servers := 50
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test that a single server is elected efficiently even when there are many servers.")

	// is a leader elected?
	cfg.checkOneLeader()

	// sleep a bit to avoid racing with followers learning of the
	// election, then check that all peers agree on the term.
	time.Sleep(100 * time.Millisecond)
	term1 := cfg.checkTerms()
	if term1 < 1 {
		t.Fatalf("term is %v, but should be at least 1", term1)
	}

	// does the leader+term stay the same if there is no network failure?
	time.Sleep(2 * RaftElectionTimeout)
	term2 := cfg.checkTerms()
	if term1 != term2 {
		fmt.Printf("warning: term changed even though there were no failures\n")
	}

	// there should still be a leader.
	cfg.checkOneLeader()

	cfg.end()
}

func TestALotOfServersAgree(t *testing.T) {
	servers := 50
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test that servers agree even when theres a lot of them.")

	time.Sleep(100 * time.Millisecond)

	iters := 3
	for index := 1; index < iters+1; index++ {
		nd, _ := cfg.nCommitted(index)
		if nd > 0 {
			t.Fatalf("some have committed before Start()")
		}

		xindex := cfg.one(index*100, servers, false)
		if xindex != index {
			t.Fatalf("got index %v but expected %v", xindex, index)
		}
	}

	cfg.end()
}

func TestALotOfServersNoLeader(t *testing.T) {
	servers := 50
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test that no leader is elected when the majority fail.")

	cfg.checkOneLeader()

	for i := 0; i < 24; i++ {
		cfg.disconnect(i)
	}

	// Disconnect the leader
	leader := cfg.checkOneLeader()
	cfg.disconnect(leader)

	// sleep for two election timeouts
	time.Sleep(2 * RaftElectionTimeout)

	// There should be no leader
	cfg.checkNoLeader()

	// Reconnect a server
	cfg.connect(0)

	// sleep for two election timeouts
	time.Sleep(2 * RaftElectionTimeout)

	// There should be one leader
	cfg.checkOneLeader()

	cfg.end()
}

func TestALotOfServersFail(t *testing.T) {
	servers := 50
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Despite multiple failures, the large number of servers should still agree.")

	time.Sleep(2 * RaftElectionTimeout)

	cfg.one(101, servers, false)

	for i := 0; i < 24; i++ {
		cfg.disconnect(i)
	}

	// the leader and remaining follower should be
	// able to agree despite the disconnected follower.
	for i := 0; i < 24; i++ {
		cfg.one(i, servers-24, false)
	}

	// re-connect
	for i := 0; i < 24; i++ {
		cfg.connect(i)
	}

	// the full set of servers should preserve
	// previous agreements, and be able to agree
	// on new commands.
	cfg.one(123123, servers, true)
	time.Sleep(RaftElectionTimeout)
	cfg.one(112312333, servers, true)
	time.Sleep(RaftElectionTimeout)
	cfg.one(2222, servers, true)
	time.Sleep(RaftElectionTimeout)
	cfg.one(12312341412, servers, true)

	cfg.end()
}

func TestALotOfServersPersist(t *testing.T) {
	servers := 50
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test that a lot of servers can efficiently persist their state.")

	cfg.one(11, servers, true)

	// crash and re-start all
	for i := 0; i < servers; i++ {
		cfg.start1(i, cfg.applier)
	}
	for i := 0; i < servers; i++ {
		cfg.disconnect(i)
		cfg.connect(i)
	}

	// timeout
	time.Sleep(2 * RaftElectionTimeout)

	cfg.one(12, servers, true)

	leader1 := cfg.checkOneLeader()
	cfg.disconnect(leader1)
	cfg.start1(leader1, cfg.applier)
	cfg.connect(leader1)

	cfg.one(13, servers, true)

	leader2 := cfg.checkOneLeader()
	cfg.disconnect(leader2)
	cfg.one(14, servers-1, true)
	cfg.start1(leader2, cfg.applier)
	cfg.connect(leader2)

	cfg.wait(4, servers, -1) // wait for leader2 to join before killing i3

	i3 := (cfg.checkOneLeader() + 1) % servers
	cfg.disconnect(i3)
	cfg.one(15, servers-1, true)
	cfg.start1(i3, cfg.applier)
	cfg.connect(i3)

	cfg.one(16, servers, true)

	// crash and re-start all
	for i := 0; i < servers; i++ {
		cfg.start1(i, cfg.applier)
	}
	for i := 0; i < servers; i++ {
		cfg.disconnect(i)
		cfg.connect(i)
	}

	// timeout
	time.Sleep(2 * RaftElectionTimeout)

	cfg.one(17, servers, true)
	cfg.one(18, servers, true)

	cfg.end()
}
