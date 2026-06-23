.PHONY: urls build up down restart logs clean status generate-data test-pre test-post test-unit

urls:
	@echo URLs
	@echo - Airflow UI: http://localhost:8080 ^(admin/admin^)
	@echo - Spark Master UI: http://localhost:8081
	@echo - Spark Worker UI: http://localhost:8082
	@echo - MinIO Console: http://localhost:9001 ^(minioadmin/minioadmin^)
	@echo - MinIO API: http://localhost:9000
	@echo - Grafana: http://localhost:3000 ^(admin/admin^)
	@echo - Metabase: http://localhost:3001 ^(create admin user on first login^)
	@echo - Analytics Postgres: localhost:5433 ^(analytics/analytics, db=motor_policy_dw^)

build:
	@echo Building Docker images
	docker compose build

up:
	@echo Starting all services
	docker compose up -d
	@echo Services are starting up

down:
	@echo Stopping all services
	docker compose down

restart: 
	@echo Restarting
	docker compose down
	docker compose up -d

logs:
	@echo Checking logs
	docker compose logs -f

status:
	@echo Checking container status
	docker compose ps

generate-data:
	@echo Resetting and regenerating data
	python generate_sample_data.py

test-pre:
	@echo Running pre-pipeline tests
	pytest tests -m "not post_pipeline" -v --tb=short

test-post:
	@echo Running post-pipeline tests
	pytest tests -m "post_pipeline" -v --tb=short

clean:
	@echo Cleaning up
	docker compose down -v
	@echo Clean-up complete
