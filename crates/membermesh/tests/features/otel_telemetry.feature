Feature: OpenTelemetry metrics and structured tracing integration

  @implemented
  Scenario: OpenTelemetry metrics record membership gauges, lifecycle events, gossip traffic, and RPC latencies
    Given an offline 3-node cluster "alice,bob,carol" wired with an in-memory OpenTelemetry meter on "bob"
    When all 3 telemetry nodes converge, "alice" updates metadata to "region=us-east", and "alice" gracefully leaves
    Then "bob"'s OpenTelemetry metric reader contains membership gauges, lifecycle event counters, gossip counters, and RPC latency histograms

  @implemented
  Scenario: OpenTelemetry admission metrics record both allowed and denied peers
    Given an offline secret-protected telemetry cluster "alice,bob" with an in-memory OpenTelemetry meter on "bob"
    When unauthorized "mallory" attempts to join via "bob" with a wrong secret
    Then "bob"'s OpenTelemetry metric reader records both allowed and denied admission checks

  @implemented
  Scenario: Structured tracing spans bridge into an OpenTelemetry TracerProvider
    Given an isolated 2-node cluster "alice,bob" instrumented with an in-memory OpenTelemetry tracer
    When "alice" and "bob" converge and "alice" gracefully leaves
    Then the OpenTelemetry span exporter captures membermesh RPC and actor lifecycle spans
