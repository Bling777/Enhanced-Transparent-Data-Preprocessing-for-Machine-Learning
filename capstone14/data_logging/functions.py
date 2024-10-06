from collections.abc import Iterable
from dataclasses import asdict
import json
import os
# import requests

import pandas

from capstone14.data_logging.pipeline_run import PipelineRun

def log_data(run: PipelineRun, description: str):
    def wrapper(func):
        def with_data_logging(*args, **kwargs):
            input_data = []
            output_data = []
            for arg in args:
                if isinstance(arg, pandas.DataFrame):
                    input_data.append(arg)
            for key in kwargs:
                if isinstance(kwargs[key], pandas.DataFrame):
                    input_data.append(kwargs[key])
            result = func(*args, **kwargs)
            if isinstance(result, pandas.DataFrame):
                output_data = [result]
            if isinstance(result, Iterable):
                for var in result:
                    if isinstance(var, pandas.DataFrame):
                        output_data.append(var)
            run.add_processing_step(
                description=description,
                input_datasets=input_data,
                output_datasets=output_data
            )
            return result
        return with_data_logging
    return wrapper


# def send_pipeline_run_to_server(run: PipelineRun, host: str, port: int):
#     body = {
#         "run_id": run.run_id,
#         "start_time": str(run.start_time),
#         "dataset_ids": [dataset["id"] for dataset in run.datasets],
#         "processing_steps": [asdict(step) for step in run.processing_steps]
#     }
#     response = requests.post(f"http://{host}:{port}/runs/", json=body)
#     if response.status_code == 200:
#         for dataset in run.datasets:
#             data_profile = dataset.get("data_profile") # type: ignore
#             body = {
#                 "id": dataset["id"],
#                 "data_profile": data_profile.as_dict() # type: ignore
#             }
#             requests.post(
#                 f"http://localhost:8000/data-profile/{run.run_id}",
#                 json=body
#             ) 


def save_pipeline_run_to_file(run: PipelineRun, path: str):
    result = {
        "run_id": run.run_id,
        "start_time": run.start_time.strftime("%c"),
        "dataset_ids": [dataset["id"] for dataset in run.datasets],
        "processing_steps": [asdict(step) for step in run.processing_steps]
    }
    datasets = [
        {
            "dataset_id": dataset.get("id", -1),
            "profile": dataset.get("data_profile", {}).as_dict()
        } 
        for dataset in run.datasets
    ]
    result["data_profiles"] = datasets
    with open(os.path.join(path, f"capstone14_{run.run_id}.json"), "w") as outfile:
        json.dump(result, outfile, indent=4)  # type: ignore 
