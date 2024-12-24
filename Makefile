.EXPORT_ALL_VARIABLES:

TAG = 0.2.0

all: lint build clean test

build:
	docker compose -f tests/resources/docker-compose.yaml build

changelog:
	gitchangelog > CHANGELOG.md

clean:
	docker compose -f tests/resources/docker-compose.yaml down -t 0

lint:
	yamllint -s .
	isort .
	flake8
	docker run --rm -i hadolint/hadolint < Dockerfile

tag:
	@echo $(TAG)

test:
	docker compose -f tests/resources/docker-compose.yaml up -d --wait
	sleep 10
	PYTHONPATH=. pytest --timeout=15
