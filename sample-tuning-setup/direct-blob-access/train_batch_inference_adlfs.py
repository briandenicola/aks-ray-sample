#!/usr/bin/env python3
# Distributed training + inference benchmark (XGBoost / LightGBM)
# — refactored so that adlfs.AzureBlobFileSystem is instantiated inside each worker.


import json
import os
import time
from typing import Dict, List
import numpy as np
import pandas as pd

import xgboost as xgb
import lightgbm as lgb
import pyarrow.parquet as pq
import pyarrow

import ray
from ray import data
from ray.train import RunConfig, ScalingConfig
from ray.train.lightgbm.v2 import LightGBMTrainer
from ray.train.xgboost.v2 import XGBoostTrainer
from ray.train.xgboost import RayTrainReportCallback as XGBoostReportCallback
from ray.train.lightgbm import RayTrainReportCallback as LightGBMReportCallback

# thresholds (unchanged)
_TRAINING_TIME_THRESHOLD = 600
_PREDICTION_TIME_THRESHOLD = 450

# small experiment set (unchanged)
_EXPERIMENT_PARAMS = {
    "smoke_test": {
        "data": (
            "https://air-example-data-2.s3.us-west-2.amazonaws.com/"
            "10G-xgboost-data.parquet/8034b2644a1d426d9be3bbfa78673dfa_000000.parquet"
        ),
        "num_workers": 1,
        "cpus_per_worker": 1,
    },
    "10G": {
        "data": "az://traineddata/testdata/10G-xgboost-data/",
        "num_workers": 3,
        "cpus_per_worker": 6,
    },
    "100G": {
        "data": "az://traineddata/testdata/100G-xgboost-data/",
        "num_workers": 10,
        "cpus_per_worker": 12,
    },
}


class BasePredictor:
    def __init__(self, report_callback_cls, result: ray.train.Result):
        self.model = report_callback_cls.get_model(result.checkpoint)

    def __call__(self, data):
        raise NotImplementedError


class XGBoostPredictor(BasePredictor):
    def __call__(self, data: pd.DataFrame) -> Dict[str, np.ndarray]:
        dmatrix = xgb.DMatrix(data)
        return {"predictions": self.model.predict(dmatrix)}


class LightGBMPredictor(BasePredictor):
    def __call__(self, data: pd.DataFrame) -> Dict[str, np.ndarray]:
        return {"predictions": self.model.predict(data)}


# -------------------------
# Helper IO utilities used INSIDE workers
# -------------------------
def normalize_azure_path(path: str) -> str:
    # Accept 'az://container/prefix', '/container/prefix', or 'container/prefix' and return 'container/prefix'.
    p = path
    if p.startswith("az://"):
        p = p[len("az://") :]
    p = p.lstrip("/")
    return p


def list_parquet_files_with_adlfs(adlfs_fs, path_prefix: str) -> List[str]:
    # List parquet files under a path prefix using adlfs filesystem.
    # Returns fully qualified paths relative to adlfs fs (i.e., 'container/dir/file.parquet').

    # normalize
    prefix = normalize_azure_path(path_prefix).rstrip("/")
    files = []

    # fsspec/adlfs ls returns entries; we recursively walk directories
    def walk(p):
        try:
            entries = adlfs_fs.ls(p, detail=True)
        except FileNotFoundError:
            return
        for e in entries:
            # adlfs returns dict or string depending on ls usage; we used detail=True
            name = e.get("name") if isinstance(e, dict) else e
            # ensure we interpret both container-root and nested paths correctly
            # If name ends with '/', treat as directory
            if name.endswith("/"):
                walk(name.rstrip("/"))
            else:
                if name.lower().endswith(".parquet"):
                    files.append(name)
    # If prefix is a file, add directly
    try:
        info = adlfs_fs.info(prefix)
        if info.get("type") == "file" and prefix.lower().endswith(".parquet"):
            return [prefix]
    except FileNotFoundError:
        # fall back to walk
        pass

    walk(prefix)
    # Sort to get reproducible assignment
    files.sort()
    return files


def read_parquet_files_via_adlfs(adlfs_fs, file_paths: List[str]) -> pd.DataFrame:
    # Read parquet files from adlfs file-like objects using pyarrow.parquet and return concatenated pandas DataFrame.
    dfs = []
    for path in file_paths:
        # open file-like object
        with adlfs_fs.open(path, "rb") as f:
            # pyarrow can read from file-like
            table = pq.read_table(f)
            dfs.append(table.to_pandas())
    if not dfs:
        # return empty DataFrame
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


# -------------------------
# Train loop functions (instantiate adlfs inside each worker)
# -------------------------
def xgboost_train_loop_function(config: Dict):
    from azure.identity.aio import DefaultAzureCredential
    import adlfs
    from ray import train

    # Instantiate adlfs inside worker (DefaultAzureCredential will pick up MSI/CLI/env)
    credential = DefaultAzureCredential()
    adlfs_fs = adlfs.AzureBlobFileSystem(account_name=config["account_name"], credential=credential)

    # get world info for file assignment
    ctx = train.get_context()
    rank = ctx.get_world_rank()  # worker index
    world_size = ctx.get_world_size()

    # list parquet files under provided path and assign a subset to this worker
    all_files = list_parquet_files_with_adlfs(adlfs_fs, config["data_path"])
    # simple round-robin assignment
    assigned = [p for i, p in enumerate(all_files) if (i % world_size) == rank]

    # read assigned files into a local pandas DataFrame
    train_df = read_parquet_files_via_adlfs(adlfs_fs, assigned)

    if train_df.empty:
        # nothing to train on this worker (rare if partitioning is skewed)
        print(f"[worker {rank}] no files assigned. Exiting training loop early.")
        return

    label_column, params = config["label_column"], config["params"]
    train_X, train_y = train_df.drop(label_column, axis=1), train_df[label_column]

    dtrain = xgb.DMatrix(train_X, label=train_y)

    report_callback = config["report_callback_cls"]
    xgb.train(
        params,
        dtrain=dtrain,
        num_boost_round=10,
        callbacks=[report_callback()]
    )


def lightgbm_train_loop_function(config: Dict):
    from azure.identity.aio import DefaultAzureCredential
    import adlfs
    from ray import train

    credential = DefaultAzureCredential()
    adlfs_fs = adlfs.AzureBlobFileSystem(account_name=config["account_name"], credential=credential)

    ctx = train.get_context()
    rank = ctx.get_world_rank()
    world_size = ctx.get_world_size()

    all_files = list_parquet_files_with_adlfs(adlfs_fs, config["data_path"])
    assigned = [p for i, p in enumerate(all_files) if (i % world_size) == rank]

    train_df = read_parquet_files_via_adlfs(adlfs_fs, assigned)

    if train_df.empty:
        print(f"[worker {rank}] no files assigned. Exiting training loop early.")
        return

    label_column, params = config["label_column"], config["params"]
    train_X, train_y = train_df.drop(label_column, axis=1), train_df[label_column]
    train_set = lgb.Dataset(train_X, label=train_y)

    report_callback = config["report_callback_cls"]
    lgb.train(
        params,
        train_set=train_set,
        num_boost_round=10,
        callbacks=[report_callback()]
    )


# -------------------------
# Framework config mapping (unchanged)
# -------------------------
_FRAMEWORK_PARAMS = {
    "xgboost": {
        "trainer_cls": XGBoostTrainer,
        "predictor_cls": XGBoostPredictor,
        "train_loop_function": xgboost_train_loop_function,
        "train_loop_config": {
            "params": {
                "objective": "binary:logistic",
                "eval_metric": ["logloss", "error"],
            },
            "label_column": "labels",
            "report_callback_cls": XGBoostReportCallback,
        },
    },
    "lightgbm": {
        "trainer_cls": LightGBMTrainer,
        "predictor_cls": LightGBMPredictor,
        "train_loop_function": lightgbm_train_loop_function,
        "train_loop_config": {
            "params": {
                "objective": "binary",
                "metric": ["binary_logloss", "binary_error"],
            },
            "label_column": "labels",
            "report_callback_cls": LightGBMReportCallback,
        },
    },
}


# -------------------------
# Training and prediction orchestration
# -------------------------
def train(framework: str, data_path: str, num_workers: int, cpus_per_worker: int, account_name: str) -> ray.train.Result:
    framework_params = _FRAMEWORK_PARAMS[framework]
    trainer_cls = framework_params["trainer_cls"]
    framework_train_loop_fn = framework_params["train_loop_function"]

    # Pass only small, serializable config to Ray; data_path & account_name will be used inside workers
    train_loop_config = framework_params["train_loop_config"].copy()
    train_loop_config.update({
        "data_path": data_path,
        "account_name": account_name,
    })

    trainer = trainer_cls(
        train_loop_per_worker=framework_train_loop_fn,
        train_loop_config=train_loop_config,
        scaling_config=ScalingConfig(
            num_workers=num_workers,
            resources_per_worker={"CPU": cpus_per_worker}
        ),
        datasets=None,  # we read files directly inside workers
        run_config=RunConfig(
            storage_path="traineddata/cluster_storage_adlfs",
            name=f"{framework}_benchmark",
            storage_filesystem=pyarrow.fs.AzureFileSystem("mittassaaccount"),
        )
    )

    result = trainer.fit()
    return result


def predict(framework: str, result: ray.train.Result, data_path: str, account_name: str):
    # Basic prediction loop that uses adlfs inside the driver to list files,
    # and dispatches predictions to Ray tasks that each load the model and run
    # prediction on assigned files (to avoid serializing adlfs or large datasets).
    import adlfs
    from ray import remote
    from azure.identity.aio import DefaultAzureCredential

    # list all parquet files using adlfs at driver (driver can create FS)
    credential = DefaultAzureCredential()
    adlfs_fs = adlfs.AzureBlobFileSystem(account_name=account_name, credential=credential)
    all_files = list_parquet_files_with_adlfs(adlfs_fs, data_path)
    all_files.sort()

    # create remote worker function
    @ray.remote(num_cpus=1)
    def predict_worker(checkpoint, files_subset):
        # each remote task creates its own adlfs FS and loads model from checkpoint
        from azure.identity.aio import DefaultAzureCredential
        import adlfs
        import pyarrow.parquet as pq
        import pandas as pd

        credential = DefaultAzureCredential()
        fs = adlfs.AzureBlobFileSystem(account_name=account_name, credential=credential)

        # load model from checkpoint using the same callback method
        predictor_cls = _FRAMEWORK_PARAMS[framework]["predictor_cls"]
        report_callback_cls = _FRAMEWORK_PARAMS[framework]["train_loop_config"]["report_callback_cls"]
        predictor = predictor_cls(report_callback_cls, checkpoint)

        # Read files and run predictions
        out_preds = []
        for path in files_subset:
            with fs.open(path, "rb") as fh:
                table = pq.read_table(fh)
                df = table.to_pandas()
                if "labels" in df.columns:
                    df = df.drop(columns=["labels"])
                preds = predictor(df)["predictions"]
                out_preds.append(preds)
        # flatten and return
        if out_preds:
            return np.concatenate(out_preds)
        return np.array([])

    # partition file list into N remote tasks (choose concurrency)
    concurrency = max(1, int(ray.cluster_resources().get("CPU", 1) // 2))
    # create simple chunks
    chunks = [all_files[i::concurrency] for i in range(concurrency)]
    # get checkpoint object
    checkpoint = result.checkpoint
    futs = [predict_worker.remote(checkpoint, chunk) for chunk in chunks if chunk]
    results = ray.get(futs)
    # combine (not used further here)
    preds = np.concatenate([r for r in results if r.size > 0]) if results else np.array([])
    print("Completed prediction, total pred count:", preds.size)


# -------------------------
# CLI / main
# -------------------------
def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("framework", type=str, choices=["xgboost", "lightgbm"], default="xgboost")
    parser.add_argument("--size", type=str, choices=["10G", "100G"], default="100G")
    parser.add_argument("--disable-check", action="store_true", help="disable runtime error on benchmark timeout")
    parser.add_argument("--smoke-test", action="store_true")
    parser.add_argument("--account-name", type=str, required=True, help="Azure storage account name (for adlfs)")
    parser.add_argument("--num-workers", type=int, default=4, help="Number of workers.")
    parser.add_argument("--cpus-per-worker", type=int, default=8, help="Number of CPUs per worker.")
    args = parser.parse_args()

    experiment = args.size if not args.smoke_test else "smoke_test"
    experiment_params = _EXPERIMENT_PARAMS[experiment]

    data_path = experiment_params["data"]
    num_workers = experiment_params["num_workers"]
    cpus_per_worker = experiment_params["cpus_per_worker"]

    if args.num_workers is not None:
        num_workers = args.num_workers
    if args.cpus_per_worker is not None:
        cpus_per_worker = args.cpus_per_worker

    print(f"Running {args.framework} training benchmark...")
    training_start = time.perf_counter()
    result = train(args.framework, data_path, num_workers, cpus_per_worker, args.account_name)
    training_time = time.perf_counter() - training_start

    print(f"Running {args.framework} prediction benchmark...")
    prediction_start = time.perf_counter()
    predict(args.framework, result, data_path, args.account_name)
    prediction_time = time.perf_counter() - prediction_start

    times = {"training_time": training_time, "prediction_time": prediction_time}
    print("Training result:\n", result)
    print("Training/prediction times:", times)
    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/result.json")
    with open(test_output_json, "wt") as f:
        json.dump(times, f)

    if not args.disable_check:
        if training_time > _TRAINING_TIME_THRESHOLD:
            raise RuntimeError(
                f"Training is taking {training_time} seconds, "
                f"which is longer than expected ({_TRAINING_TIME_THRESHOLD} seconds)."
            )
        if prediction_time > _PREDICTION_TIME_THRESHOLD:
            raise RuntimeError(
                f"Batch prediction is taking {prediction_time} seconds, "
                f"which is longer than expected ({_PREDICTION_TIME_THRESHOLD} seconds)."
            )


if __name__ == "__main__":
    main()
