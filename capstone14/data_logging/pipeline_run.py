from dataclasses import dataclass
from datetime import datetime
from uuid import uuid4

import pandas

from capstone14.data_profiling.data_profile import DataProfile


@dataclass
class ProcessingStep:
    id: str
    description: str
    input_datasets: list[str]
    output_datasets: list[str]


class PipelineRun:
    def __init__(self, input_data: pandas.DataFrame | None = None):
        self.run_id = str(uuid4())
        self.start_time: datetime = datetime.now()
        self.datasets: list[dict] = []
        if input_data is not None:
            self.add_dataset(input_data)
        self.processing_steps: list[ProcessingStep] = []

    def add_dataset(self, dataset: pandas.DataFrame) -> str:
        data_profile = DataProfile(dataset)
        new_dataset = {
            "id": data_profile.hash,
            "raw": dataset,
            "data_profile": data_profile
        }
        self.datasets.append(new_dataset)
        return str(new_dataset["id"])

    def get_data_profile_of_dataset(self, dataset_id: int) -> DataProfile | None:
        for dataset in self.datasets:
            if dataset["id"] == dataset_id:
                return dataset["data_profile"]
        return None

    def search_datasets(self, dataset_to_compare: pandas.DataFrame) -> str | None:
        if not self.datasets:
            return None
        try:    
            dataset = next(filter(
                lambda dataset: dataset["raw"].equals(dataset_to_compare), 
                self.datasets
            ))
            return dataset["id"]
        except (StopIteration, ValueError):
            return None

    def add_processing_step(
            self, 
            description: str, 
            input_datasets: list[pandas.DataFrame], 
            output_datasets: list[pandas.DataFrame]) -> ProcessingStep:
        input_ids = []
        for input_dataset in input_datasets:
            input_id = self.search_datasets(input_dataset)
            if not input_id:
                input_id = self.add_dataset(input_dataset)
            input_ids.append(input_id)

        output_ids = []
        for output_dataset in output_datasets:
            output_id = self.add_dataset(output_dataset)
            output_ids.append(output_id)
        
        processing_step = ProcessingStep(
            id=str(uuid4()),
            description=description,
            input_datasets=input_ids,
            output_datasets=output_ids
        )
        self.processing_steps.append(processing_step)
        return processing_step
