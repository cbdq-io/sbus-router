.EXPORT_ALL_VARIABLES:

TAG = $$( python -c 'import router; print(router.__version__)' )

all: lint build clean test

build:
	docker compose -f tests/resources/docker-compose.yaml build

changelog:
	docker run --quiet --rm --volume "${PWD}:/mnt/source" --workdir /mnt/source ghcr.io/cbdq-io/gitchangelog > CHANGELOG.md

clean:
	docker compose -f tests/resources/docker-compose.yaml down -t 0 --remove-orphans

lint:
	yamllint -s .
	isort .
	flake8
	bandit -qr .
	docker run --rm -i hadolint/hadolint < Dockerfile

tag:
	@echo $(TAG)

test:
	docker compose -f tests/resources/docker-compose.yaml up -d --wait
	PYTHONPATH=. pytest --timeout=15
