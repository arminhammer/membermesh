Feature: Workspace and BDD harness smoke check
  As a library maintainer
  I want the Cucumber BDD runner and workspace crates wired cleanly
  So that every subsequent feature can follow red-first BDD

  @implemented
  Scenario: Core crate reports its version
    Given the membermesh-core library is loaded
    When I query the crate version
    Then a non-empty version string is returned
