# A Zipkin tracing plugin for Cassandra

Cassandra-3.4 provides [pluggable tracing](http://www.planetcassandra.org/blog/cassandra-3-4-release-overview/). By adding 3 jar files to the Cassandra classpath and one jvm option, Cassandra's tracing is replaced with Zipkin. It can even identify incoming Zipkin traces and add Cassandra's own internal tracing on to it.

# Installation

    mvn install
    cp target/*.jar $CASSANDRA_HOME/lib/

Then start Cassandra with

    JVM_OPTS \
      ="-Dcassandra.custom_tracing_class=com.thelastpickle.cassandra.tracing.ZipkinTracing" \
        cassandra

Or edit the `jvm.options`.

The default SpanCollector sends the tracing messages via HTTP to `http://127.0.0.1:9411/`. This is the default port for the [zipkin-java](https://github.com/openzipkin/zipkin-java) server. The same url is used for the UI.

## Continuing existing Zipkin traces

To continue existing Zipkin traces from application code through the DataStax CQL driver and into the Cassandra cluster.

The Cassandra nodes need to be started also with the `cassandra.custom_query_handler_class` jvm option to a query handler that accepts incoming payloads over the CQL protocol:

    JVM_OPTS \
      ="-Dcassandra.custom_tracing_class=com.thelastpickle.cassandra.tracing.ZipkinTracing" \
        -Dcassandra.custom_query_handler_class=org.apache.cassandra.cql3.CustomPayloadMirroringQueryHandler"
      cassandra

(Or edit the `jvm.options`)

Then in the application code where the DataStax CQL driver is used put the Zipkin traceId and spanId into the *outgoing payload* like

    SpanId spanId = clientTracer.startNewSpan(statement.toString());

    ByteBuffer traceHeaders = ByteBuffer.wrap(spanId.bytes());

    statement.setOutgoingPayload(singletonMap("zipkin", traceHeaders.array()));

    clientTracer.setCurrentClientServiceName(serviceName);
    clientTracer.setClientSent();
    ResultSet result = session.execute(statement);
    clientTracer.setClientReceived();
    return result;


## More information

See this [presentation](http://thelastpickle.com/files/2015-09-24-using-zipkin-for-full-stack-tracing-including-cassandra/presentation/tlp-reveal.js/tlp-cassandra-zipkin.html).

## Background

 See [CASSANDRA-10392](https://issues.apache.org/jira/browse/CASSANDRA-10392) for the patch to extend Cassandra's tracing that this project plugs into.

## Troubleshooting

When this tracing is used instead of Cassandra's default tracing, any cqlsh statements run after enabling tracing with
`TRACING ON;` are going to time out eventually giving

    Unable to fetch query trace: Trace information was not available within …

This is because cqlsh is polling for tracing information in system_traces which isn't any longer being created. Zipkin tracing doesn't support this interaction with cqlsh (it's more of a thing to use with a tracing sampling rate). Improvements in this area are possible though, for example we could use zipkin tracing when the custom payload contains a zipkin traceId and spanId and fall back to normal tracing otherwise (which would work for cqlsh interaction). For the meantime an easy fix around this behaviour in cqlsh is to reduce Session.max_trace_wait down to 1 second.

