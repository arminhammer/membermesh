Feature: Optional LAN multicast auto-discovery and mesh convergence

  @implemented
  Scenario: Two unbootstrapped nodes discover each other via LAN auto-discovery and converge
    Given a loopback auto-discovery port for the scenario
    When node "alice" starts with auto-discovery on topic "disco-alpha" without bootstrap tickets
    And node "bob" starts with auto-discovery on topic "disco-alpha" without bootstrap tickets
    Then nodes "alice,bob" converge on a 2-node cluster with a shared advisory leader

  @implemented
  Scenario: Nodes on different topics ignore each other's discovery beacons
    Given a loopback auto-discovery port for the scenario
    When node "alice" starts with auto-discovery on topic "disco-topic-a" without bootstrap tickets
    And node "bob" starts with auto-discovery on topic "disco-topic-b" without bootstrap tickets
    And the discovery cluster waits 1500 milliseconds
    Then discovery node "alice" sees 1 member in its cluster
    And discovery node "bob" sees 1 member in its cluster

  @implemented
  Scenario: Ticket bootstrap and auto-discovery coexist in the same cluster
    Given a loopback auto-discovery port for the scenario
    When node "alice" starts with auto-discovery on topic "disco-coexist" without bootstrap tickets
    And node "bob" starts on topic "disco-coexist" bootstrapping from "alice" without auto-discovery
    And node "carol" starts with auto-discovery on topic "disco-coexist" without bootstrap tickets
    Then nodes "alice,bob,carol" converge on a 3-node cluster with a shared advisory leader

  @implemented
  Scenario: SharedSecretAdmission rejects unauthorized auto-discovered peers
    Given a loopback auto-discovery port for the scenario
    When node "alice" starts with auto-discovery on topic "disco-secret" and passphrase "correct-secret"
    And node "bob" starts with auto-discovery on topic "disco-secret" and passphrase "correct-secret"
    And node "mallory" starts with auto-discovery on topic "disco-secret" and passphrase "wrong-secret"
    Then discovery nodes "alice,bob" converge on a 2-node cluster excluding "mallory"
    And discovery node "mallory" never joins "alice" or "bob" within 1500 milliseconds
