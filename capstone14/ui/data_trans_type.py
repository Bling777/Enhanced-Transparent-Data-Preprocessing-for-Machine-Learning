from enum import Enum
from collections.abc import Iterable
from pandas import DataFrame
from capstone14.data_logging.pipeline_run import PipelineRun
from capstone14.data_logging.functions import generate_description

class DataTransType(Enum):
    # value, number of input datasets, function name
    MERGE = "Merge", 2, "merge"
    DEDUPLICATE = "Deduplicate", 1, "deduplicate"
    IMPUTE = "Impute Missing Values", 1, "impute"

    def __new__(cls, *args, **kwds):
        obj = object.__new__(cls)
        obj._value_ = args[0]
        return obj

    # ignore the first param since it's already set by __new__
    def __init__(self, _: str, num_input: int = 1, func: str = None):
        self._num_input_ = num_input
        self._func_ = func

    def __str__(self):
        return self.value

    # this makes sure that the description is read-only
    @property
    def num_input(self):
        return self._num_input_
    
    @property
    def func(self):
        return self._func_


def run_data_transformation(run:PipelineRun, trans_type: DataTransType, input_dataset_ids, *args, **kwargs):
    args_trans = [run.get_dataset(id) for id in input_dataset_ids]
    if len(args):
        args_trans.append(args)
    # print(args_trans)
    
    # Execute the function
    result = eval(trans_type.func)(*args_trans, **kwargs)
    output_dataset_id = run.add_dataset(result)

    # Generate description using LLM
    description = generate_description(eval(trans_type.func), args_trans, kwargs)
                    
    # Create and add the processing step
    run.add_processing_step_with_dataset_ids(
        description=description,
        input_dataset_ids=input_dataset_ids,
        output_dataset_id=output_dataset_id
    )

    return output_dataset_id


def deduplicate(df: DataFrame) -> DataFrame:
      return df.drop_duplicates()

def impute(df: DataFrame) -> DataFrame:
      return df

def merge(df1: DataFrame, df2: DataFrame) -> DataFrame:
      return df1

