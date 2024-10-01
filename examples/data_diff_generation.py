import json
from os.path import abspath, dirname, join

from pandas import DataFrame, read_csv
from sklearn.compose import ColumnTransformer
from sklearn.impute import KNNImputer, SimpleImputer

from capstone14.data_profiling.data_profile import DataProfile


def impute_missing_values(df: DataFrame) -> DataFrame:
    knn_imputer = KNNImputer(n_neighbors=3, weights="uniform")
    simple_imputer = SimpleImputer(strategy="most_frequent")
    transformers = ColumnTransformer(
        transformers=[
            ("imputation_num_features", knn_imputer, ["age", "salary"]),
            ("imputation_cat_features", simple_imputer, ["gender"])
        ],
        remainder="passthrough",
        verbose_feature_names_out=False
    ).set_output(transform="pandas")
    return transformers.fit_transform(df)

if __name__ == "__main__":
    current_dir = dirname(abspath((__file__)))
    df = read_csv(join(current_dir, "datasets", "employees_v1.csv"))
    imputed = impute_missing_values(df)
    dp = DataProfile(df)
    diff = dp.calculate_diff(DataProfile(imputed))
    with open("data_diff.json", "w") as outfile:
        json.dump(diff, outfile, indent=4)
