import argparse
from os.path import abspath, dirname, join

from pandas import DataFrame, read_csv
from sklearn.compose import ColumnTransformer
from sklearn.impute import KNNImputer, SimpleImputer

# from capstone14 import (PipelineRun, log_data, save_pipeline_run_to_file,
#                   send_pipeline_run_to_server)
from capstone14.data_logging.pipeline_run import PipelineRun
from capstone14.data_logging.functions import log_data, save_pipeline_run_to_file
from capstone14.db.db_functions import create_run

current_dir = dirname(abspath((__file__)))
df = read_csv(join(current_dir, "datasets", "netflix.csv"))

run = PipelineRun(df)

@log_data(run, "deduplication")
def deduplicate(df: DataFrame) -> DataFrame:
    return df.drop_duplicates()


@log_data(run, "missing_value_imputation")
def impute_missing_values(df: DataFrame) -> DataFrame:
    knn_imputer = KNNImputer(n_neighbors=3, weights="uniform")
    simple_imputer = SimpleImputer(missing_values="Not Given", strategy="most_frequent")
    transformers = ColumnTransformer(
        transformers=[
            ("imputation_num_features", knn_imputer, ["release_year"]),
            ("imputation_cat_features", simple_imputer, 
             ["show_id", "type", "director", "country", "rating", "duration", "listed_in"])
        ],
        remainder="passthrough",
        verbose_feature_names_out=False
    ).set_output(transform="pandas")
    return transformers.fit_transform(df)


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description="Test data logging")
    arg_parser.add_argument(
        "--server", 
        action="store_true", 
        help="Send collected data to server (it is assumed the API is running at localhost:8080)"
    )
    arg_parser.add_argument("--save", action="store_true", help="Save collected data to JSON file")
    args = arg_parser.parse_args()
    
    df.pipe(deduplicate) \
      .pipe(impute_missing_values)

    if args.server:
        # send_pipeline_run_to_server(run, host="localhost", port=8000)
        create_run(run)
    if args.save:
        save_pipeline_run_to_file(run, ".")
