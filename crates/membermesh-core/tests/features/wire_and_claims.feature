Feature: Core domain types, signed claims, and wire framing
  As a membership protocol engine
  I want peer claims cryptographically signed and encoded in bounded frames
  So that forged claims are rejected and iroh-gossip deduplication does not drop rebroadcasts

  @implemented
  Scenario: Valid signed claim round-trips through a gossip envelope
    Given a peer "alice" with a valid Ed25519 signer
    And a valid alive claim for "alice" at join sequence 1
    When "alice" signs the claim and encodes it into a gossip envelope with nonce 1
    And the envelope is decoded and verified
    Then verification succeeds and returns the claim for "alice"

  @implemented
  Scenario: Tampered claim payload fails verification
    Given a peer "alice" with a valid Ed25519 signer
    And a valid alive claim for "alice" at join sequence 1
    When "alice" signs the claim and its join sequence is tampered to 99
    And the tampered claim is verified
    Then verification fails with an invalid signature error

  @implemented
  Scenario: Claim signed by a different peer than the subject is rejected
    Given a peer "alice" with a valid Ed25519 signer
    And a peer "mallory" with a valid Ed25519 signer
    When "mallory" attempts to sign a claim claiming subject "alice"
    Then signing fails with a subject mismatch error

  @implemented
  Scenario: Oversized metadata is rejected
    When I construct member metadata of 513 bytes
    Then metadata construction fails with a metadata too large error

  @implemented
  Scenario: Consecutive encodings of identical claims produce distinct wire bytes
    Given a peer "alice" with a valid Ed25519 signer
    And a valid alive claim for "alice" at join sequence 1
    When "alice" encodes the same signed claim with nonces 1 and 2
    Then the two wire byte payloads are distinct

  @implemented
  Scenario: Unknown wire version and oversized frames fail without panicking
    When I decode a gossip envelope with wire version 99
    Then decoding fails with an unsupported version error
    When I decode a frame exceeding the maximum wire size
    Then decoding fails with a frame too large error
