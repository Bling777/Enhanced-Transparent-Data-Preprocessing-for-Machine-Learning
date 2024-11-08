import numpy
import pandas


# General stats 
def missing_rate(column: pandas.Series) -> float:
    return float(column.isna().sum() / column.size)


# Stats for categorical features
def num_of_categories(column: pandas.Series) -> int:
    return int(column.unique().size)


def mode(column: pandas.Series) -> str: 
    return column.mode(dropna=True)[0]


def frequency_distribution(column: pandas.Series) -> dict:
    return {
        key: float(value) 
        for key, value in column.value_counts().to_dict().items()
    }


# Stats for numeric features
def min(column: pandas.Series) -> float:
    return float(column.min())


def max(column: pandas.Series) -> float:
    return float(column.max())


def mean(column: pandas.Series) -> float:
    return float(column.mean())


def median(column: pandas.Series) -> float:
    return float(column.median())


def std(column: pandas.Series) -> float:
    return float(column.std())


def skewness(column: pandas.Series) -> float:
    return float(column.skew()) # type: ignore


def kurtosis(column: pandas.Series) -> float:
    return float(column.kurtosis()) # type: ignore


def mad(column: pandas.Series) -> float:
    mad = (column - column.median()).abs().median()
    return float(mad)


def histogram(column: pandas.Series) -> dict:
    hist = {}
    try:
        unique_values = column.dropna().unique()
        if len(unique_values) < 10:
            num_bins = len(unique_values)
        else:
            num_bins = 10 # TODO: research what is a good range
        bins, edges = numpy.histogram(column.dropna(), bins=num_bins)
        hist["bins"] = [float(bin) for bin in bins.tolist()]
        hist["edges"] = [float(edge) for edge in edges.tolist()]
    except ValueError:
        print(f"Generation of histogram was not possible for column {column.name}")
    return hist


STAT_COLUMN_GENERAL = {
    "missing_rate": missing_rate
}

STAT_COLUMN_NUMERIC = {
    "min": min,
    "max": max,
    "mean": mean,
    "median": median,
    "std": std,
    "mad": mad,
    "skewness": skewness,
    "kurtosis": kurtosis,
    "histogram": histogram,
}

STAT_COLUMN_CATEGORICAL = {
    "num_of_categories": num_of_categories,
    "mode": mode,
    "frequency_distribution": frequency_distribution
}
