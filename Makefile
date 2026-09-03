.PHONY: install db db-down server client test test-server test-client

install:
	python -m pip install -r server/requirements.txt
	npm --prefix client install

db:
	docker compose up -d postgres

db-down:
	docker compose down

server:
	python -m uvicorn server.app:app --reload --port 8000

client:
	npm --prefix client run dev

test: test-server test-client

test-server:
	python -m pytest server/tests

test-client:
	npm --prefix client test -- --run
