Feature: Composable Router attachment, EventStream lifecycle notifications, and admission policies

  @implemented
  Scenario: Caller-managed Endpoint and Router share membermesh with a custom application ALPN
    Given an offline composable cluster "alice,bob" sharing a custom "/app/echo/1" protocol
    Then both composable nodes converge on the ordered roster "alice,bob"
    And "alice" can still invoke the custom "/app/echo/1" protocol on "bob"

  @implemented
  Scenario: Typed EventStream emits Joined, Updated, LeaderChanged, and Left events
    Given an offline composable 2-node cluster "alice,bob" with EventStream subscription on "bob"
    When "alice" updates its metadata to "role=primary"
    And "alice" gracefully leaves the composable cluster
    Then "bob"'s EventStream observes Updated, LeaderChanged, and Left for "alice"

  @implemented
  Scenario: AllowListAdmission rejects unlisted peers across both gossip and sync
    Given an offline allowlist-protected cluster allowing "alice,bob" but excluding "mallory"
    When "mallory" attempts to join via "bob"
    Then "alice" and "bob" include "alice,bob" and exclude "mallory"

  @implemented
  Scenario: SharedSecretAdmission validates HMAC tokens and rejects wrong secrets
    Given an offline secret-protected cluster "alice,bob" using passphrase "correct-horse-battery-staple"
    When "mallory" attempts to join via "bob" with passphrase "wrong-passphrase"
    Then "alice" and "bob" include "alice,bob" and exclude "mallory"
