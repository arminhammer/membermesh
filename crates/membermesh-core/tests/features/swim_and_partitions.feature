Feature: Sans-I/O SWIM + Lifeguard failure detection, partitions, and iroh-gossip co-simulation
  As a distributed membership engine
  I want SWIM probing, Lifeguard local-health awareness, and anti-entropy sync
  So that crashes fail over cleanly, asymmetric links don't false-positive, and partitions converge

  @implemented
  Scenario: Silent crash is detected via SWIM and fails over leadership
    Given a simulated LAN cluster of peers "alice,bob,carol"
    When the cluster stabilizes for 5000 ms
    And peer "alice" silently crashes
    And the simulation advances by 10000 ms
    Then "bob" and "carol" see the active roster "bob,carol"
    And "bob" and "carol" elect "bob" as leader

  @implemented
  Scenario: Indirect PingReq rescues an asymmetric link without false death
    Given a simulated LAN cluster of peers "alice,bob,carol"
    When the cluster stabilizes for 5000 ms
    And the direct link between "alice" and "bob" is severed symmetrically while "carol" can reach both
    And the simulation advances by 12000 ms
    Then "alice" still sees "bob" in the active roster
    And "bob" still sees "alice" in the active roster

  @implemented
  Scenario: Locally stalled node avoids false eviction via Lifeguard LHA
    Given a simulated LAN cluster of peers "alice,bob,carol"
    When the cluster stabilizes for 5000 ms
    And "bob" experiences a local CPU stall for 1800 ms
    And the simulation advances by 10000 ms
    Then all 3 peers remain alive with "alice" as leader

  @implemented
  Scenario: Partitioned cluster suppresses minority quorum and heals via anti-entropy
    Given a simulated LAN cluster of peers "alice,bob,carol,dave,eve"
    When the cluster stabilizes for 6000 ms
    And the cluster is partitioned into "alice,bob" and "carol,dave,eve"
    And the simulation advances by 12000 ms
    Then the minority partition "alice,bob" has no quorum 3 leader
    And the majority partition "carol,dave,eve" elects "carol" with quorum 3
    When the partition heals and the simulation advances by 12000 ms
    Then all 5 simulated peers converge on "alice,bob,carol,dave,eve" with leader "alice"

  @implemented
  Scenario: GossipLagged triggers immediate anti-entropy recovery
    Given a simulated LAN cluster of peers "alice,bob,carol"
    When the cluster stabilizes for 5000 ms
    And "carol" drops incoming gossip while "dave" joins via "alice"
    And "carol" receives a GossipLagged signal and advances by 500 ms
    Then "carol" recovers "dave" via PushPullSync in the ordered roster "alice,bob,carol,dave"

  @implemented
  Scenario: Eight-node co-simulation on real iroh-gossip HyParView and PlumTree converges and fails over
    Given an iroh-gossip co-simulated cluster of 8 peers
    When the co-simulation advances by 15000 ms
    Then all 8 co-simulated peers agree on the identical 8-peer order and oldest leader
    When the oldest co-simulated peer crashes and the co-simulation advances by 15000 ms
    Then the remaining 7 co-simulated peers agree on the second-oldest peer as leader
