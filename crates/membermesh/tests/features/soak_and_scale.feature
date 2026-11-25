@slow
Feature: Multi-node scale, long-horizon churn soak, and live rolling-restart resilience

  @slow
  Scenario: 20-node virtual-clock churn and partition storm converges with bounded tombstones
    Given a virtual 20-node SimCluster running for 1000 virtual seconds with partitions and churn
    Then all surviving virtual nodes converge on the identical ordered roster and leader
    And reaped tombstone tables remain bounded below 4 entries per survivor

  @slow
  Scenario: 8-node live offline iroh rolling-restart chain hands off leadership cleanly
    Given an offline 8-node membermesh cluster "n1,n2,n3,n4,n5,n6,n7,n8" for rolling restart
    When the current leader gracefully leaves and rejoins sequentially 3 times
    Then all 8 live nodes remain converged on the updated join order
    And the live trace invariant checker confirms zero violations
