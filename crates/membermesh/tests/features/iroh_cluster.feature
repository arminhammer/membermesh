Feature: Multi-node iroh-gossip mesh integration and live trace invariant verification
  As a distributed application using membermesh over iroh
  I want peers to join, converge on order and leadership, detect crashes, and heal partitions over real QUIC and iroh-gossip
  So that cluster membership and advisory leader election work reliably end-to-end

  @implemented
  Scenario: Five-node offline iroh cluster bootstraps sequentially and converges on order and leader
    Given an offline 5-node membermesh cluster "alice,bob,carol,dave,eve" bootstrapped sequentially
    Then all 5 live nodes converge on the ordered roster "alice,bob,carol,dave,eve"
    And all 5 live nodes elect "alice" as leader
    And the live trace invariant checker confirms zero violations

  @implemented
  Scenario: Graceful leave by the leader promotes the second-oldest live node
    Given an offline 5-node membermesh cluster "alice,bob,carol,dave,eve" bootstrapped sequentially
    When all 5 live nodes have converged
    And live node "alice" gracefully leaves
    Then the remaining 4 live nodes converge on "bob,carol,dave,eve" with leader "bob"
    And the live trace invariant checker confirms zero violations

  @implemented
  Scenario: Abrupt crash of a live node is detected via SWIM and fails over leadership
    Given an offline 5-node membermesh cluster "alice,bob,carol,dave,eve" bootstrapped sequentially
    When all 5 live nodes have converged
    And live node "alice" abruptly crashes
    Then the remaining 4 live nodes detect the failure and converge on "bob,carol,dave,eve" with leader "bob"
    And the live trace invariant checker confirms zero violations

  @implemented
  Scenario: Partitioned live iroh cluster suppresses minority quorum and heals cleanly
    Given an offline 5-node membermesh cluster "alice,bob,carol,dave,eve" bootstrapped sequentially
    When all 5 live nodes have converged
    And the live cluster is partitioned into "alice,bob" and "carol,dave,eve"
    Then the live minority "alice,bob" has no quorum 3 leader while "carol,dave,eve" elects "carol" with quorum 3
    When the live partition heals
    Then all 5 live nodes reconverge on "alice,bob,carol,dave,eve" with leader "alice"
    And the live trace invariant checker confirms zero violations

  @implemented
  Scenario: Late joiner bootstrapping from a non-leader learns full roster and oldest leader
    Given an offline 4-node membermesh cluster "alice,bob,carol,dave" bootstrapped sequentially
    When all 4 live nodes have converged
    And live node "eve" joins bootstrapping only from "dave"
    Then all 5 live nodes converge on the ordered roster "alice,bob,carol,dave,eve"
    And all 5 live nodes elect "alice" as leader
    And the live trace invariant checker confirms zero violations
