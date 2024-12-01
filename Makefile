.EXPORT_ALL_VARIABLES:

TAG = 0.1.0

all: lint build clean test

build:
	docker compose -f tests/resources/docker-compose.yaml build

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
	PYTHONPATH=. pytest --timeout=5
