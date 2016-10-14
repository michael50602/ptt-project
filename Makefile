BUCKET_NAME = ptt-source-posu-cto-1
PROJ_ID = ptt-project 
ptt-parse-raw-data-cloud:
	python parse_pipeline.py --worker_machine_type n1-standard-8 --num_workers 3 --project $(PROJ_ID) --staging_location gs://$(BUCKET_NAME)/binary --temp_location gs://$(BUCKET_NAME)/tmp --job_name parse-ptt-data --runner BlockingDataflowPipelineRunner --output=gs://$(BUCKET_NAME)/pipeline_output/ 
ptt-parse-raw-data-local:
	python parse_pipeline.py
