To use zipkin tracing in Cassandra
 - build this project,
 - update a few jar files in $CASSANDRA_HOME/lib
 - then start cassandra with
    -Dcassandra.custom_tracing_class=com.thelastpickle.cassandra.tracing.ZipkinTracing
    -Dcassandra.custom_query_handler_class=org.apache.cassandra.cql3.CustomPayloadMirroringQueryHandler


## Building

```
mvn clean install
```

## Installing

```
cp target/cassandra-zipkin-tracing-*.jar $CASSANDRA_HOME/lib/
rm $CASSANDRA_HOME/lib/commons-codec-1.2.jar

# check the following dependencies match those used in pom.xml
cp /.m2/repository/com/github/kristofa/brave-core/3.0.0-rc-1/brave-core-3.0.0-rc-1.jar $CASSANDRA_HOME/lib/
cp  $CASSANDRA_HOME/lib/
cp  $CASSANDRA_HOME/lib/
cp  $CASSANDRA_HOME/lib/
```

## Running

```
$CASSANDRA_HOME/bin/cassandra -Dcassandra.custom_tracing_class=com.thelastpickle.cassandra.tracing.ZipkinTracing -Dcassandra.custom_query_handler_class=org.apache.cassandra.cql3.CustomPayloadMirroringQueryHandler
```
