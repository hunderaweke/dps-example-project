Feature: Example REST API
  As an API consumer
  I want to create and read examples
  So that they are stored and processed asynchronously

  Scenario: Create an example
    When I create an example with name "first example" and owner "acc_1"
    Then the response status should be 201
    And the response field "status" should be "pending"
    And an example.created event should be published for the created example

  Scenario: Fetch a created example
    Given I create an example with name "fetch me" and owner "acc_2"
    When I fetch the created example
    Then the response status should be 200
    And the response field "name" should be "fetch me"

  Scenario: Reject invalid input
    When I create an example with name "x" and owner ""
    Then the response status should be 422

  Scenario: Unknown example
    When I fetch an example with a random id
    Then the response status should be 404

  Scenario: List examples
    Given I create an example with name "listed one" and owner "acc_3"
    When I list examples
    Then the response status should be 200
    And the list should contain at least 1 item
