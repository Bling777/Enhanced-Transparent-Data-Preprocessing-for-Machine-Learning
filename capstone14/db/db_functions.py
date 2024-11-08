from pymongo import MongoClient
from capstone14.data_logging.pipeline_run import PipelineRun
from capstone14.data_profiling.data_profile import DataProfile
from dataclasses import asdict
from typing import List
from datetime import datetime

ATLAS_URI = "mongodb+srv://Lee:capstone14@cluster0.tljn8.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"


db_client = MongoClient(ATLAS_URI)
db = db_client.get_database("capstone14") # open database "capstone14"


# save the pipleline run
# @app.post("/runs/")
def create_run(run: PipelineRun):
    print(f"save {run.run_id}")
    run_collection = db.get_collection("pipeline_run")
    run_collection.insert_one({
        "run_id": run.run_id,
        "start_time": run.start_time,
        "dataset_ids": [dataset["id"] for dataset in run.datasets],
        "processing_steps": [asdict(step) for step in run.processing_steps]
    })
    data_profile_collection = db.create_collection(run.run_id, check_exists=True)
    for dataset in run.datasets:
        if data_profile_collection.count_documents({ "dataset_id": dataset["id"] }, limit=1) == 0:
            data_profile_collection.insert_one({
                "dataset_id": dataset["id"],
                "profile": dataset["data_profile"].as_dict()
            })
        else:
            print("Data profile does already exist")
            
def get_available_runs():
    """
    Get all available pipeline runs from the MongoDB database.
    Returns a list of run dictionaries.
    """
    try:
        # Get the pipeline_run collection
        run_collection = db.get_collection("pipeline_run")
        
        # Find all documents in the collection
        runs = run_collection.find({})
        
        # Convert cursor to list of dictionaries
        result = []
        for run in runs:
            run_dict = {
                'run_id': str(run['run_id']),  # Ensure run_id is string
                'start_time': run['start_time'],
                'dataset_ids': run['dataset_ids'],
                'processing_steps': run['processing_steps']
            }
            result.append(run_dict)
            
        return result
        
    except Exception as e:
        print(f"Database error in get_available_runs: {str(e)}")
        raise


