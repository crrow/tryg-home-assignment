# A simple key-value-timestamp store

Key-value-timestamp store Implement a key-value-timestamp store in whichever programming language you prefer. You are encouraged to use any frameworks and libraries you deem appropriate.

## Specification

1. Data should be persisted through process restarts (stored on disk).

2. The application should support PUT and GET apis. While the data does not need to be exposed over a network, this is a language agnostic example to illustrate how the application could be called:

    1: `curl -X PUT http://localhost:8080 -H 'Content-Type: application/json' -d '{"key": "mykey", "value": "myvalue", "timestamp" : 1673524092123456}'`

    2: `curl -X GET http://localhost:8080 -H 'Content-Type: application/json' -d '{"key":"mykey", "timestamp": 1673524092123456}'` # returns "myvalue"

3. Lookups should consider the key-timestamp combination. For example, given the stored sequence:`[{key: "mykey", timestamp: 100, value: "value1"}, {key: "mykey", timestamp: 101, value: "value2"}]` a request for "mykey" at timestamp 99 should return nothing, a request at timestamp 100 should return "value1" and a request at timestamp 101 or higher should return "value2".

4. Your solution should have defined outcomes for concurrent API calls.

5. Document how to build and run your solution from the command line. For example: "go run main.go".