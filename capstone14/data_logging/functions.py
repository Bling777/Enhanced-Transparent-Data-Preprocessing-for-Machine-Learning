from collections.abc import Iterable
from dataclasses import asdict
import json
import os
import requests
from datetime import datetime
import pandas
from dataclasses import dataclass
import inspect
from typing import Any, Callable, TypeVar
from functools import wraps
import openai

from capstone14.data_logging.pipeline_run import PipelineRun

@dataclass
class ProcessingStep:
    def __init__(self, id, description, input_datasets, output_datasets):
        self.id = id
        self.description = description
        self.input_datasets = input_datasets
        self.output_datasets = output_datasets
        

    def to_dict(self):
        return {
            'id': self.id,
            'description': self.description,
            'input_datasets': self.input_datasets,
            'output_datasets': self.output_datasets,
            
        }

def generate_description(func: Callable, args: tuple, kwargs: dict) -> str:
    """
    Generate a description of the processing step using the function's source code
    and context through LLM.
    """
    # Get the function's source code
    source = inspect.getsource(func)
    
    # Get the function's signature
    sig = inspect.signature(func)
    
    # Create a context string with argument values
    arg_names = list(sig.parameters.keys())
    arg_values = args[:len(arg_names)]
    args_context = dict(zip(arg_names, arg_values))
    args_context.update(kwargs)
    
    # Prepare the prompt for the language model
    prompt = f"""
    Function name: {func.__name__}
    Function source code: {source}
    Arguments: {args_context}
    
    Please provide a concise description of what this data processing step does.
    """
    
    try:
        #print("call openAI")
        # Use OpenAI API to generate description
        response = openai.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{
                "role": "user",
                "content": prompt
            }]
        )
        #print(response)
        return response.choices[0].message.content.strip()
    except Exception as e:
        # Fallback to a basic description if LLM fails
        print(e)
        return f"Processing step: {func.__name__}"
    


def log_data(run: PipelineRun):
    def wrapper(func: Callable) -> Callable:
        @wraps(func)
        def with_data_logging(*args: Any, **kwargs: Any) -> Any:
            # Automatically collect input data
            input_data = []
            for arg in args:
                if isinstance(arg, pandas.DataFrame):
                    input_data.append(arg)
            for value in kwargs.values():
                if isinstance(value, pandas.DataFrame):
                    input_data.append(value)
            
            # Execute the function
            result = func(*args, **kwargs)
            
            # Automatically collect output data
            output_data = []
            if isinstance(result, pandas.DataFrame):
                output_data = [result]
            elif isinstance(result, Iterable):
                output_data = [df for df in result if isinstance(df, pandas.DataFrame)]
            
            # Generate description using LLM
            #print("data log")
            description = generate_description(func, args, kwargs)
            #print(description)
                    
            # Create and add the processing step
            step = run.add_processing_step(
                description=description,
                input_datasets=input_data,
                output_datasets=output_data,
            )
            
            return result
        return with_data_logging
    return wrapper

def send_pipeline_run_to_server(run: PipelineRun, host: str, port: int):
    print("Preparing to send pipeline run...")
    
    body = {
        "run_id": run.run_id,
        "start_time": str(run.start_time),  # Consider using a datetime format if appropriate
        "dataset_ids": [dataset["id"] for dataset in run.datasets],
        "processing_steps": [asdict(step) for step in run.processing_steps]
    }
    
    print("Payload for POST /runs/:", body)

    try:
        # Optional: Check if the server is reachable
        response1 = requests.get(f"http://{host}:{port}/runs/")
        print(f"GET /runs/ status code: {response1.status_code}")
        
        # Send POST request to create a new pipeline run
        response = requests.post(f"http://{host}:{port}/runs/", json=body)
        print(f"POST /runs/ status code: {response.status_code}")

        if response.status_code == 200:
            # Handle success case
            print("Pipeline run created successfully.")
            for dataset in run.datasets:
                data_profile = dataset.get("data_profile")  # type: ignore
                profile_body = {
                    "id": dataset["id"],
                    "data_profile": data_profile.as_dict()  # type: ignore
                }
                profile_response = requests.post(
                    f"http://{host}:{port}/data-profile/{run.run_id}",
                    json=profile_body
                )
                print(f"POST /data-profile/{run.run_id} status code: {profile_response.status_code}")
                if profile_response.status_code != 200:
                    print(f"Failed to send data profile for {dataset['id']}: {profile_response.status_code}")
        else:
            print(f"Failed to create pipeline run: {response.status_code}, {response.text}")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")

def save_pipeline_run_to_file(run: PipelineRun, path: str):
    result = {
        "run_id": run.run_id,
        "start_time": run.start_time,
        "dataset_ids": [dataset["id"] for dataset in run.datasets],
        "processing_steps": run.processing_steps
    }
    datasets = [
        {
            "dataset_id": dataset.get("id", -1),
            # "profile": dataset.get("data_profile", {})
        } 
        for dataset in run.datasets
    ]
    result["data_profiles"] = datasets
    # Define the file path
    file_path = os.path.join(path, f"capstone14_{run.run_id}.json")  # Construct the file path
    result = convert_datetimes(result)
   
    #print(  result["processing_steps"] )
    #print(type(result["processing_steps"]))
    #for step in result["processing_steps"]:
        #print(type(step), step)

# Open the file in write mode and use json.dump
    with open(file_path, 'w') as fp:
        json.dump(result, fp,default=serialize_custom, indent=4)  # Pass the file object to json.dump

# Utility functions        
def convert_datetimes(obj):
    if isinstance(obj, dict):
        return {k: convert_datetimes(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_datetimes(i) for i in obj]
    elif isinstance(obj, datetime):
        return obj.isoformat()
    return obj
def convert_processing_steps(obj):
    if isinstance(obj, dict):
        return {k: convert_processing_steps(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_processing_steps(i) for i in obj]
    elif isinstance(obj, ProcessingStep):
        return obj.to_dict()  # Convert to dictionary
    return obj
def serialize_custom(obj):
    if isinstance(obj, ProcessingStep):
        # Convert the ProcessingStep object to a dictionary
        return {
            "id": obj.id,
            "description": obj.description,
            "input_datasets": obj.input_datasets,
            "output_datasets": obj.output_datasets
        }
    elif isinstance(obj, datetime):  # Handling datetime objects
        return obj.isoformat()
    elif hasattr(obj, "__dict__"):  # Generic case for objects with attributes
        return obj.__dict__
    else:
        return str(obj)  # Fallback to string representation if nothing else works

"""
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
"""

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

"""
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
"""
