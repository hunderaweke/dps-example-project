Feature: Health and schema
  The API starts against a fresh database, applies the migrations and
  reports its dependencies.

  Scenario: Liveness
    When I request "GET" "/healthz"
    Then the response status should be 200
    And the response field "status" should be "ok"

  Scenario: Readiness checks Postgres
    When I request "GET" "/readyz"
    Then the response status should be 200
    And the readiness check "postgres" should be "ok"

  Scenario: The audit schema is migrated
    Then the "audit_records" table should exist
