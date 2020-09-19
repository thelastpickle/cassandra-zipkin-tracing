
all: setup test teardown
	

test:
	docker exec -t resources_cassandra-0_1 cqlsh -e "create keyspace test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
	docker exec -t resources_cassandra-0_1 cqlsh -e "create table test.test (one text primary key, two text);"
	docker exec -t resources_cassandra-0_1 cqlsh -e "tracing on; insert into test.test (one , two ) VALUES ( 'one', 'two');" || true
	docker exec -t resources_cassandra-0_1 cqlsh -e "tracing on; select * from test.test;" || true
	docker exec -t resources_zipkin-0_1 wget "http://localhost:9411/api/v2/traces?limit=10" -q --header "accept: application/json" -O - | grep -q "parsing insert"
	docker exec -t resources_zipkin-0_1 wget "http://localhost:9411/api/v2/traces?limit=10" -q --header "accept: application/json" -O - | grep -q "parsing select"


setup:
	docker-compose up --build -d
	docker-compose ps
	sleep 20

teardown:
	docker-compose down
