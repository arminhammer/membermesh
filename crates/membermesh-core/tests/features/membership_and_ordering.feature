Feature: Membership CRDT, deterministic join order, peer age, and advisory leader election
  As a distributed cluster node
  I want membership states to merge deterministically by join sequence and incarnation
  So that all converged peers agree on member order, age, and the oldest alive leader

  @implemented
  Scenario: Sequential joins produce identical oldest-to-newest order across all peers
    Given peers "alice", "bob", and "carol" join sequentially
    When their membership tables synchronize in reverse delivery order
    Then every peer sees the ordered roster "alice,bob,carol"
    And every peer elects "alice" as leader

  @implemented
  Scenario: Oldest peer keeps seniority after refuting suspicion
    Given peers "alice", "bob", and "carol" have converged
    When "bob" suspects "alice" at incarnation 1
    And "alice" processes the suspicion alert and broadcasts its refutation
    Then "alice" increments incarnation to 2 while keeping join sequence 1
    And all peers still elect "alice" as leader

  @implemented
  Scenario: Graceful leave promotes the second-oldest peer
    Given peers "alice", "bob", and "carol" have converged
    When "alice" gracefully leaves the cluster
    And the leave claim propagates to "bob" and "carol"
    Then "bob" and "carol" see the ordered roster "bob,carol"
    And "bob" and "carol" elect "bob" as leader

  @implemented
  Scenario: Rejoined peer moves to the back of the join order
    Given peers "alice", "bob", and "carol" have converged
    When "alice" gracefully leaves the cluster and restarts with a new session
    And the restarted "alice" rejoins via "bob"
    Then every peer sees the ordered roster "bob,carol,alice"
    And every peer elects "bob" as leader

  @implemented
  Scenario: Partitioned peer with the same session refutes dead and keeps seniority
    Given peers "alice", "bob", and "carol" have converged
    When "bob" marks "alice" dead at incarnation 1
    And "alice" still running its original session receives the dead claim
    Then "alice" refutes with incarnation 2 and retains join sequence 1
    And after sync "bob" and "carol" restore "alice" as leader

  @implemented
  Scenario: Simultaneous founders break ties deterministically by PeerId
    Given "alice" and "bob" independently found clusters at join sequence 1
    When "alice" and "bob" synchronize snapshots
    Then both peers agree on the identical ordered roster and single leader

  @implemented
  Scenario: Skewed wall clocks do not alter join order or leader selection
    Given peer "alice" joined at sequence 1 with wall clock 9999999999999
    And peer "bob" joined at sequence 2 with wall clock 1000
    When both peers synchronize
    Then both peers elect "alice" as leader despite skewed wall clocks

  @implemented
  Scenario: Quorum guard suppresses leader when alive count is below threshold
    Given peers "alice", "bob", and "carol" have converged
    When "bob" and "carol" are marked dead on "alice"
    Then "alice" sees advisory leader "alice"
    But "alice" with quorum 2 sees no leader

  @implemented
  Scenario: Reaped tombstones are not resurrected by stale syncs
    Given peers "alice" and "bob" have converged
    When "bob" is marked dead at time 1000 and reaped at time 70000
    And a stale snapshot containing "bob" at the dead session is merged
    Then "bob" remains absent from the ordered roster
