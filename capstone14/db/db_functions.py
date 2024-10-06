from pymongo import MongoClient
from capstone14.data_logging.pipeline_run import PipelineRun
from capstone14.data_profiling.data_profile import DataProfile
from dataclasses import asdict


ATLAS_URI = "mongodb+srv://Lee:capstone14@cluster0.tljn8.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"


db_client = MongoClient(ATLAS_URI)
db = db_client.get_database("capstone14") # open database "capstone14"


# save the pipleline run
# @app.post("/runs/")
def create_run(run: PipelineRun):
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


