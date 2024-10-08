from dataclasses import dataclass
from datetime import datetime
from uuid import uuid4
import inspect
import pandas
from typing import Optional, Union, List

from capstone14.data_profiling.data_profile import DataProfile


@dataclass
class ProcessingStep:
    id: str
    description: str
    input_datasets: list[str]
    output_datasets: list[str]

class PipelineRun:
    def __init__(self, input_data: Optional[pandas.DataFrame] = None):
        self.run_id = str(uuid4())
        self.start_time: datetime = datetime.now()
        self.datasets: list[dict] = []
        if input_data is not None:
            self.add_dataset(input_data)
        self.processing_steps: list[ProcessingStep] = []
        self.analysis_context: dict = {}  # Store context about the pipeline

    def add_dataset(self, dataset: pandas.DataFrame) -> str:
        data_profile = DataProfile(dataset)
        dataset_info = {
            "id": data_profile.hash,
            "raw": dataset,
            "data_profile": data_profile,
            "schema": self._extract_schema(dataset),
            "created_at": datetime.now()
        }
        self.datasets.append(dataset_info)
        return str(dataset_info["id"])
    
    def _extract_schema(self, dataset: pandas.DataFrame) -> dict:
        """Extract and store schema information about the dataset"""
        return {
            "columns": list(dataset.columns),
            "dtypes": dataset.dtypes.to_dict(),
            "shape": dataset.shape
        }

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
            input_datasets: List[pandas.DataFrame], 
            output_datasets: List[pandas.DataFrame]) -> ProcessingStep:
                    
        # Process input datasets
        input_ids = []
        for input_dataset in input_datasets:
            input_id = self.search_datasets(input_dataset)
            if not input_id:
                input_id = self.add_dataset(input_dataset)
            input_ids.append(input_id)

        # Process output datasets
        output_ids = []
        for output_dataset in output_datasets:
            output_id = self.add_dataset(output_dataset)
            output_ids.append(output_id)
        
        # Create the processing step with enhanced metadata
        processing_step = ProcessingStep(
            id=str(uuid4()),
            description=description,
            input_datasets=input_ids,
            output_datasets=output_ids
        )
        
        # Update analysis context
        self._update_analysis_context(processing_step)
        
        self.processing_steps.append(processing_step)
        print(processing_step)
        return processing_step

    def _update_analysis_context(self, step: ProcessingStep):
        """Update the analysis context with information about the processing step"""
        self.analysis_context[step.id] = {
            "timestamp": datetime.now(),
            "input_schemas": [
                self._get_dataset_schema(dataset_id) 
                for dataset_id in step.input_datasets
            ],
            "output_schemas": [
                self._get_dataset_schema(dataset_id) 
                for dataset_id in step.output_datasets
            ],
            "description": step.description
        }

    def _get_dataset_schema(self, dataset_id: str) -> Optional[dict]:
        """Retrieve schema information for a dataset"""
        for dataset in self.datasets:
            if dataset["id"] == dataset_id:
                return self._extract_schema(dataset["raw"])
        return None
