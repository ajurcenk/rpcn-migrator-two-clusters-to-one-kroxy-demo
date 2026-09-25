.PHONY: step1-verify filter-test step2 step2-up step2-test step2-down \
	step3 step3-negative step3-up step3-seed step3-migrators step3-test step3-negative-test step3-down clean

MVN = docker run --rm -u $$(id -u):$$(id -g) -e MAVEN_CONFIG=/var/maven/.m2 \
	-v $$HOME/.m2:/var/maven/.m2:z -v $(CURDIR)/kroxylicious-filter:/build:z -w /build \
	maven:3.9-eclipse-temurin-21 mvn -B -Duser.home=/var/maven

STEP2 = docker compose -f compose/docker-compose.step2.yaml
STEP3 = docker compose -f compose/docker-compose.step3.yaml --profile migrators
GOTEST3 = cd tests && go test -tags step3 -count=1 -v ./...

step1-verify:
	./scripts/step1-verify.sh

# Unit tests for the Kroxylicious filter (builds target/consumer-group-prefix.jar).
filter-test:
	$(MVN) package

# Unit tests, then the proxy integration test against a fresh stack; always tears down.
step2: filter-test
	$(MAKE) step2-up
	$(MAKE) step2-test; status=$$?; $(MAKE) step2-down; exit $$status

step2-up:
	$(STEP2) up -d --build --wait

step2-test:
	cd tests && go test -tags step2 -run TestStep2 -count=1 -v ./...
	./scripts/step2-check-proxy-logs.sh compose/docker-compose.step2.yaml

step2-down:
	$(STEP2) down -v

# Three clusters, two proxies, two migrators (published Connect 4.100.0 image). Brokers and
# proxies healthy -> seed -> migrators -> assertions and log checks; always tears down.
step3:
	$(MAKE) step3-down
	$(MAKE) step3-up step3-seed step3-migrators && $(MAKE) step3-test; status=$$?; $(MAKE) step3-down; exit $$status

# Negative control: same scenario, both migrators write straight to redpanda-dest.
step3-negative:
	$(MAKE) step3-down
	$(MAKE) step3-up step3-seed && DEST_BROKERS=redpanda-dest:9092 $(MAKE) step3-migrators && $(MAKE) step3-negative-test; \
		status=$$?; $(MAKE) step3-down; exit $$status

step3-up:
	$(STEP3) up -d --build --wait redpanda-a redpanda-b redpanda-dest kroxylicious-a kroxylicious-b

step3-seed:
	$(GOTEST3) -run TestStep3Seed

step3-migrators:
	$(STEP3) up -d migrator-a migrator-b

step3-test:
	$(GOTEST3) -run 'TestStep3Replicated|TestStep3LiveSync'
	./scripts/step3-check-logs.sh compose/docker-compose.step3.yaml

step3-negative-test:
	$(GOTEST3) -run TestStep3NegativeControl
	./scripts/step3-check-logs.sh compose/docker-compose.step3.yaml negative

step3-down:
	$(STEP3) down -v

clean: step2-down step3-down
	rm -rf kroxylicious-filter/target
