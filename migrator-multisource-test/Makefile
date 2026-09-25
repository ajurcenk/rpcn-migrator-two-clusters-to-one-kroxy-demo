.PHONY: step1-verify filter-test step2 step2-up step2-test step2-down clean

MVN = docker run --rm -u $$(id -u):$$(id -g) -e MAVEN_CONFIG=/var/maven/.m2 \
	-v $$HOME/.m2:/var/maven/.m2:z -v $(CURDIR)/kroxylicious-filter:/build:z -w /build \
	maven:3.9-eclipse-temurin-21 mvn -B -Duser.home=/var/maven

STEP2 = docker compose -f compose/docker-compose.step2.yaml

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

clean: step2-down
	rm -rf kroxylicious-filter/target
