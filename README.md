A Zipkin tracing plugin for cassandra.

 See CASSANDRA-10392 for the patch to extend Cassandra's tracing that this project plugs into.


When this tracing is used instead of Cassandra's default tracing, any cqlsh statements run after enabling tracing with
`TRACING ON;` are going to time out eventually giving

    Unable to fetch query trace: Trace information was not available within …

This is because cqlsh is polling for tracing information in system_traces which isn't any longer being created.
Zipkin tracing doesn't support this interaction with cqlsh (it's more of a thing to use with a tracing sampling rate).
Improvements in this area are possible though, for example we could use zipkin tracing when the custom payload contains
a zipkin traceId and spanId and fall back to normal tracing otherwise (which would work for cqlsh interaction).
For the meantime an easy fix around this behaviour in cqlsh is to reduce Session.max_trace_wait down to 1 second.

