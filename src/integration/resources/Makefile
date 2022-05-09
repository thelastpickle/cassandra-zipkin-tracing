
all: setup test teardown
	

test:
	docker exec -t cassandra-zipkin-tracing-tests_cassandra-00_1 cqlsh -e "create keyspace test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
	sleep 2
	docker exec -t cassandra-zipkin-tracing-tests_cassandra-00_1 cqlsh -e "create table test.test (one text primary key, two text);"
	sleep 2
	docker exec -t cassandra-zipkin-tracing-tests_cassandra-00_1 cqlsh -e "tracing on; insert into test.test (one , two ) VALUES ( 'one', 'two');" || true
	sleep 2
	docker exec -t cassandra-zipkin-tracing-tests_cassandra-00_1 cqlsh -e "tracing on; select * from test.test;" || true
	sleep 2
	docker exec -t cassandra-zipkin-tracing-tests_zipkin-00_1 wget "http://localhost:9411/api/v2/traces?limit=10" -q --header "accept: application/json" -O - | jq .
	docker exec -t cassandra-zipkin-tracing-tests_zipkin-00_1 wget "http://localhost:9411/api/v2/traces?limit=10" -q --header "accept: application/json" -O - | grep -q "parsing insert"
	docker exec -t cassandra-zipkin-tracing-tests_zipkin-00_1 wget "http://localhost:9411/api/v2/traces?limit=10" -q --header "accept: application/json" -O - | grep -q "parsing select"
	docker exec -t cassandra-zipkin-tracing-tests_zipkin-00_1 wget "http://localhost:9411/api/v2/traces?limit=10" -q --header "accept: application/json" -O - | grep -q "range_req message received from"


setup:
	docker-compose up --build -d
	while (! docker-compose ps | grep -q "cassandra-00") || docker-compose ps | grep -q -e "(health: starting)" -e "running (starting)" || docker-compose ps | grep -q "Exit" ; do docker-compose ps ; echo "waiting 10s…" ; sleep 10 ; done

teardown:
	docker-compose down
