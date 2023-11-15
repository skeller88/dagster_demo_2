import json
import os
import random

import pandas as pd
from dagster import asset, MaterializeResult, MetadataValue


@asset
def test_phrases() -> None:
    _test_phrases = [
        {
            "test_phrase": f"test_{random.randint(0, 20_000)}",
            "pipeline": random.choice(["pulse", "rate", "thermal"])
        }
    for _ in range(2000)]

    os.makedirs("data", exist_ok=True)
    with open("data/test_phrases.json", "w") as f:
        json.dump(_test_phrases, f)


@asset(deps=[test_phrases])
def tests_dataframe() -> None:
    with open("data/test_phrases.json", "r") as f:
        _test_phrases = json.load(f)

    df = pd.DataFrame(_test_phrases)
    df.to_csv("data/tests.csv")


@asset(deps=[tests_dataframe])
def most_frequent_pipelines() -> None:
    tests = pd.read_csv("data/tests.csv")
    counts: pd.Series = tests.groupby("pipeline").count()["test_phrase"]
    counts = counts.rename("num_tests")

    with open("data/most_frequent_pipelines.json", "w") as f:
        json.dump(counts.to_dict(), f)

    return MaterializeResult(
        metadata={
            "num_records": len(counts),
            "preview": MetadataValue.md(counts.to_markdown()),
        }
    )
