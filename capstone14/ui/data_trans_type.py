from enum import Enum
from collections.abc import Iterable
from pandas import DataFrame
from sklearn.compose import ColumnTransformer
from sklearn.impute import KNNImputer, SimpleImputer
from sklearn.preprocessing import StandardScaler, OrdinalEncoder
from capstone14.data_logging.pipeline_run import PipelineRun
from capstone14.data_logging.functions import generate_description
from capstone14.data_profiling.base_types import FeatureType
from capstone14.data_profiling.data_profile import infer_feature_type

import json

class DataTransType(Enum):
    # value, number of input datasets, run function name, column check function name
    DEDUPLICATE = 'Deduplicate', 1
    IMPUTE = 'Impute Missing Values', 1
    MERGE = 'Merge', 2
    STANDARDIZE = 'Standardization', 1
    ENCODE = 'Encode Category Features', 1

    def __new__(cls, *args, **kwds):
        obj = object.__new__(cls)
        obj._value_ = args[0]
        return obj

    # ignore the first param since it's already set by __new__
    def __init__(self, _: str, num_input: int = 1):
        self._num_input_ = num_input

    def __str__(self):
        return self.value

    # this makes sure that the description is read-only
    @property
    def num_input(self):
        return self._num_input_
    

def run_data_transformation(run:PipelineRun, trans_type: DataTransType, input_dataset_ids, ref_cols_1, ref_cols_2):
    # check the number of inputs
    if len(input_dataset_ids) != trans_type.num_input:
         return None
    
    print("#START transformation")
    
    # Execute the function
    result_df = None
    description = ''
    if trans_type == DataTransType.DEDUPLICATE:
        input_df = run.get_dataset(input_dataset_ids[0])
        print(input_df.head())
        result_df = deduplicate(input_df)
        print(result_df.head())
        description = generate_description(deduplicate, (input_df), {})

    elif trans_type == DataTransType.IMPUTE:
        input_df = run.get_dataset(input_dataset_ids[0])
        print(input_df.head())
        result_df = impute(input_df, ref_cols_1)
        print(result_df.head())
        description = generate_description(impute, (input_df, ref_cols_1), {})
    
    elif trans_type == DataTransType.MERGE:
        input_df_1 = run.get_dataset(input_dataset_ids[0])
        input_df_2 = run.get_dataset(input_dataset_ids[1])
        print(input_df_1.head())
        print(input_df_2.head())
        result_df = merge(input_df_1, input_df_2, ref_cols_1, ref_cols_2)
        print(result_df.head())
        description = generate_description(merge, (input_df_1, input_df_2, ref_cols_1, ref_cols_2), {})

    elif trans_type == DataTransType.STANDARDIZE:
        input_df = run.get_dataset(input_dataset_ids[0])
        print(input_df.head())
        result_df = standardize(input_df, ref_cols_1)
        print(result_df.head())
        description = generate_description(standardize, (input_df), {})

    elif trans_type == DataTransType.ENCODE:
        input_df = run.get_dataset(input_dataset_ids[0])
        print(input_df.head())
        result_df = encode(input_df, ref_cols_1)
        print(result_df.head())
        description = generate_description(standardize, (input_df), {})

    print("#END transformation")

    if result_df is None or result_df.empty:
        return None
    
    output_dataset_id = run.add_dataset(result_df)

    # Create and add the processing step
    run.add_processing_step_with_dataset_ids(
        description=description,
        input_dataset_ids=input_dataset_ids,
        output_dataset_id=output_dataset_id
    )

    return output_dataset_id


def check_columns(trans_type: DataTransType, input_cols_1: list, input_cols_2: list, 
                  ref_cols_1: list, ref_cols_2: list) -> list | None:
    if trans_type == DataTransType.MERGE:
        return check_columns_merge(input_cols_1, input_cols_2, ref_cols_1, ref_cols_2)
    return input_cols_1


def deduplicate(input_df: DataFrame) -> DataFrame:
    return input_df.drop_duplicates()


def impute(input_df: DataFrame, missing_val_cols: list) -> DataFrame:
    col_names_numeric = []
    col_names_non_numeric = []
    for col_name in missing_val_cols:
        if infer_feature_type(input_df[col_name]) == FeatureType.NUMERIC:
            col_names_numeric.append(col_name)
        else:
            col_names_non_numeric.append(col_name)

    # print(input_df.dtypes)
    print(col_names_non_numeric)
    print(col_names_numeric)
    
    knn_imputer = KNNImputer(n_neighbors=3, weights="uniform")
    simple_imputer = SimpleImputer(strategy="most_frequent")
    transformers = ColumnTransformer(
        transformers=[
            ("imputation_num_features", knn_imputer, col_names_numeric),
            ("imputation_cat_features", simple_imputer, col_names_non_numeric)
        ],
        remainder="passthrough",
        verbose_feature_names_out=False
    ).set_output(transform="pandas")
    return transformers.fit_transform(input_df)


def merge(input_df_1: DataFrame, input_df_2: DataFrame, ref_cols_1: list, ref_cols_2: list) -> DataFrame | None:
    if len(ref_cols_1) != len(ref_cols_2):
        return None
    return input_df_1.merge(input_df_2, left_on=ref_cols_1, right_on=ref_cols_2)


def standardize(input_df: DataFrame, to_standardize_cols: list) -> DataFrame:
    if len(to_standardize_cols) == 0:
        to_standardize_cols = list(input_df.columns)

    col_names_numeric = []
    for col_name in to_standardize_cols:
        if infer_feature_type(input_df[col_name]) == FeatureType.NUMERIC:
            col_names_numeric.append(col_name)

    # print(input_df.dtypes)
    print(col_names_numeric)
    
    scaler = StandardScaler().set_output(transform="pandas")
    transformers = ColumnTransformer(
        transformers=[("standardization_num_features", scaler, col_names_numeric)],
        remainder="passthrough",
        verbose_feature_names_out=False
    ).set_output(transform="pandas")
    return transformers.fit_transform(input_df)


def encode(input_df: DataFrame, to_encode_cols: list) -> DataFrame:
    if len(to_encode_cols) == 0:
        to_encode_cols = list(input_df.columns)

    col_names_category = []
    for col_name in to_encode_cols:
        if infer_feature_type(input_df[col_name]) == FeatureType.CATEGORICAL:
            col_names_category.append(col_name)

    # print(input_df.dtypes)
    print(col_names_category)
    
    scaler = OrdinalEncoder().set_output(transform="pandas")
    transformers = ColumnTransformer(
        transformers=[("encoding_category_features", scaler, col_names_category)],
        remainder="passthrough",
        verbose_feature_names_out=False
    ).set_output(transform="pandas")
    return transformers.fit_transform(input_df)


def check_columns_merge(input_cols_1: list, input_cols_2: list, ref_cols_1: list, ref_cols_2: list) -> list | None:
    if len(ref_cols_1) == 0 or len(ref_cols_1) != len(ref_cols_2):
        return None
    
    df_temp_1 = DataFrame([[1] * len(input_cols_1)], columns=input_cols_1)
    df_temp_2 = DataFrame([[1] * len(input_cols_2)], columns=input_cols_2)
    df_merged = merge(df_temp_1, df_temp_2, ref_cols_1, ref_cols_2)
    return list(df_merged.columns)

