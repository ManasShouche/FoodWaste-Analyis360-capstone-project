.PHONY: generate run dashboard test install setup

install:
	pip install -r requirements.txt

setup:
	python setup_aws.py

generate:
	python ingestion/data_generator.py

run:
	astro dev run dags trigger food_waste_pipeline

dashboard:
	streamlit run dashboard/app.py

test:
	pytest tests/ -v
