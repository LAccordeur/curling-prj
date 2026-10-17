.PHONY: help control-db-up control-db-init exchange-up exchange-init api private-executor client-read client-write smoke-test collect-logs bootstrap-node

help:
	@echo "Targets:"
	@echo "  control-db-up       Start PostgreSQL on node1"
	@echo "  control-db-init     Initialize DB schema on node1"
	@echo "  exchange-up         Start MinIO + ElasticMQ on node2"
	@echo "  exchange-init       Initialize buckets and queue on node2"
	@echo "  api                 Start FastAPI on node1"
	@echo "  private-executor    Start executor on node3"
	@echo "  client-read         Run read client on node4"
	@echo "  client-write        Run write client on node4"
	@echo "  smoke-test          Run smoke tests from current node"
	@echo "  collect-logs        Collect logs"
	@echo "  bootstrap-node ROLE=<control|exchange|private|client>"

control-db-up:
	cd deploy/control-plane && docker compose up -d

control-db-init:
	bash init_db.sh

exchange-up:
	cd deploy/exchange && docker compose up -d

exchange-init:
	bash deploy/exchange/init_exchange.sh

api:
	bash deploy/control-plane/start.sh

private-executor:
	bash deploy/private/start.sh

client-read:
	bash deploy/client/run_read.sh

client-write:
	bash deploy/client/run_write.sh

smoke-test:
	bash deploy/scripts/smoke_test.sh

collect-logs:
	bash deploy/scripts/collect_logs.sh

bootstrap-node:
	bash bootstrap_node.sh $(ROLE)
