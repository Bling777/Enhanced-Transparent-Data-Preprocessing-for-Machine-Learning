from typing import Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from pydantic import BaseModel

# ATLAS_URI = "mongodb+srv://Lee:capstone14@cluster0.tljn8.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

FEATURE_TYPES = ["CATEGORICAL", "NUMERIC"]
EXCLUDED_STATS = ["histogram", "frequency_distribution"]


# interfacing data model for ProcessingStep objects
class ProcessingStepModel(BaseModel):
    id: str
    description: str
    input_datasets: list[str]  # list[input_ids]
    output_datasets: list[str] # list[output_ids]


# interfacing data model for PipelineRun objects
class PipelineRunModel(BaseModel):
    run_id: str
    start_time: str
    # dataset_ids: list[int]
    # processing_steps: list[ProcessingStepModel]
    dataset_ids: list[str] # id is string not integer
    processing_steps: list[dict[str, Any]] # ProcessingStepModel is not json compatible


#interfacing data model for DataProfile objects
class DatasetModel(BaseModel):
    # id: int
    id: str # id is string not integer
    data_profile: dict[str, Any]


# start FastAPI
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)
db_client = MongoClient("localhost", 27017) # mongodb client
# db_client = MongoClient(ATLAS_URI)
db = db_client.get_database("capstone14") # open database "hawk"


# return pipleline runs from DB
@app.get("/runs/")
def read_pipeline_runs():
    run_collection = db.get_collection("pipeline_run")
    runs = {
        "runs": [run for run in run_collection.find(projection={"_id": False})]
    }
    return runs


# return the pipeline run of the run_id from DB
@app.get("/runs/{run_id}")
def read_pipeline_run(run_id: str):
    run_collection = db.get_collection("pipeline_run")
    return run_collection.find_one({"run_id": run_id}, {"_id": False})


# save the pipleline run
@app.post("/runs/")
def create_run(run: PipelineRunModel):
    run_collection = db.get_collection("pipeline_run")
    run_collection.insert_one({
        "run_id": run.run_id,
        "start_time": run.start_time,
        "dataset_ids": run.dataset_ids,
        "processing_steps": run.processing_steps
    })
    db.create_collection(run.run_id, check_exists=True)


# save the data profile in the pipeline run of run_id
@app.post("/data-profile/{run_id}")
def create_data_profile(run_id: str, dataset: DatasetModel):
    data_profile_collection = db.get_collection(run_id)
    if data_profile_collection.count_documents({ "dataset": dataset.id }, limit=1) == 0:
        data_profile_collection.insert_one({
            "dataset_id": dataset.id,
            "profile": dataset.data_profile
        })
    else:
        print("Data profile does already exist")


# get data profile of dataset_id
@app.get("/data-profile/{run_id}/{dataset_id}")
def read_data_profile(run_id: str, dataset_id: int):
    data_profile_collection = db.get_collection(run_id)
    return data_profile_collection.find_one(
        {"dataset_id": dataset_id},
        {"_id": False, "dataset_id": False, "profile": True}
    )


# get column information of data profile of dataset_id
@app.get("/column-info/{run_id}/{dataset_id}")
def get_column_info(run_id: str, dataset_id: int):
    data_profile_collection = db.get_collection(run_id)
    result = data_profile_collection.find_one(
        {"dataset_id": dataset_id},
        {"_id": False, "profile": True}
    )
    if not result:
        return 404
    data_profile = result.get("profile")
    columns = data_profile.get("columns", None)
    if not columns or not isinstance(columns, list):
        return 404
    
    column_info_by_type = {}
    for feature_type in FEATURE_TYPES:
        columns_of_type = list(
            filter(lambda column: column.get("feature_type") == feature_type, columns) 
        )
        if not columns_of_type:
            continue
        header = ["Name", "Internal type"] + \
                [stat for stat in columns_of_type[0].get("stats") if stat not in EXCLUDED_STATS]
        data = []
        for column in columns_of_type:
            info = []
            info.append(column.get("name"))
            info.append(column.get("internal_dtype"))
            stats = column.get("stats")
            info.extend(stats.get(key) for key in stats if key not in EXCLUDED_STATS)
            data.append(info)
        column_info_by_type[feature_type] = {
            "header": header,
            "metadata": data
        }
    return column_info_by_type
