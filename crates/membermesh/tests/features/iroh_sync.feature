Feature: Iroh Ed25519 signer, /membermesh/1 control RPCs, and PartitionMatrix cluster harness
  As a mesh transport adapter
  I want direct QUIC streams for SWIM Ping, indirect PingReq, and Push/Pull anti-entropy sync
  So that peers probe liveness and exchange full state safely alongside iroh-gossip

  @implemented
  Scenario: Direct QUIC Ping and Ack succeeds between offline iroh endpoints
    Given an offline local iroh cluster with peers "alice" and "bob"
    When "alice" sends a direct Ping to "bob" over /membermesh/1
    Then "alice" receives a matching Ack from "bob"

  @implemented
  Scenario: Indirect PingReq succeeds through a relay when direct link is blocked
    Given an offline local iroh cluster with peers "alice", "bob", and "carol"
    When the local iroh cluster blocks the link between "alice" and "bob"
    And "alice" sends an indirect PingReq for "bob" via "carol"
    Then "alice" receives a matching Ack relayed through "carol"

  @implemented
  Scenario: PushPullSync merges disjoint membership tables in one QUIC round trip
    Given an offline local iroh cluster with peers "alice" and "bob"
    And "alice" has peer "carol" in its table while "bob" has peer "dave"
    When "alice" executes a PushPullSync with "bob" over /membermesh/1
    Then both "alice" and "bob" contain "alice", "bob", "carol", and "dave"

  @implemented
  Scenario: Oversized or malformed control frames are rejected cleanly
    Given an offline local iroh cluster with peers "alice" and "bob"
    When "alice" sends an oversized control frame to "bob"
    Then "bob" rejects the frame cleanly without wedging valid subsequent Pings
