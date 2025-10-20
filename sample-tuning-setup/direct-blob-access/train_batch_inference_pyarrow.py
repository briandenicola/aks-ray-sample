import json
import numpy as np
import os
import pandas as pd
import time
from typing import Dict
import pyarrow
import io

import xgboost as xgb
import lightgbm as lgb

import ray
from ray import data
from ray.train.lightgbm import (
    LightGBMTrainer,
    RayTrainReportCallback as LightGBMReportCallback,
)
from ray.train.xgboost import (
    RayTrainReportCallback as XGBoostReportCallback,
    XGBoostTrainer,
)
from ray.train import RunConfig, ScalingConfig

# Interface Adapter that supports Runconfig storage_filesystem
# interface for adlfs
class RayAzureFSAdapter(pyarrow.fs.FileSystem):
    def __init__(self, adlfs_fs):
        self.fs = adlfs_fs
        self.account_name = adlfs_fs.account_name

    def type_name(self):
        # Used by Ray Train's StorageContext for logging/debugging
        return "azure"

    # ------------- Core API Methods for Ray -------------

    def open_input_stream(self, path: str) -> io.BufferedReader:
        # Open a blob for reading (binary mode).
        path = path.lstrip("/")
        return self.fs.open(path, "rb")

    def open_output_stream(self, path: str) -> io.BufferedWriter:
        # Open a blob for writing (binary mode).
        path = path.lstrip("/")
        container = path.split("/", 1)[0]

        # Ensure container exists
        if not self.fs.exists(container):
            self.fs.mkdir(container, exist_ok=True)

        return self.fs.open(path, "wb")

    def exists(self, path: str) -> bool:
        # Check if blob or prefix exists.
        return self.fs.exists(path.lstrip("/"))

    def delete(self, path: str, recursive: bool = False):
        # Delete blob or directory.
        self.fs.rm(path.lstrip("/"), recursive=recursive)

    def get_file_info(self, paths):
        if isinstance(paths, str):
            paths = [paths]

        infos = []
        for path in paths:
            path_clean = path.lstrip("/")
            try:
                info = self._fs.info(path_clean)
                type_map = {"file": pa_fs.FileType.File, "directory": pa_fs.FileType.Directory}
                infos.append(pa_fs.FileInfo(
                    path=path,
                    type=type_map.get(info["type"], pa_fs.FileType.Unknown),
                    size=info.get("size", 0)
                ))
            except FileNotFoundError:
                infos.append(pa_fs.FileInfo(path=path, type=pa_fs.FileType.NotFound))
        return infos if len(infos) > 1 else infos[0]

    # ------------- Optional Convenience Methods -------------

    def listdir(self, path: str):
        # List blobs/prefixes under a path
        return self.fs.ls(path.lstrip("/"))

    def mkdir(self, path: str, exist_ok: bool = True):
        # Create container or a directory marker.
        # Works for both HNS and non-HNS accounts.
        path = path.strip("/")
        if not path:
            return

        parts = path.split("/", 1)
        container = parts[0]

        # Ensure container exists
        if not self.fs.exists(container):
            self.fs.mkdir(container, exist_ok=exist_ok)

        # If nested folder path provided
        if len(parts) == 2:
            nested = parts[1].rstrip("/")
            full_prefix = f"{container}/{nested}"
            self.fs.mkdir(full_prefix, exist_ok=exist_ok)
        return path

    def create_dir(self, path: str):
        # Ray compatibility alias for mkdir.
        normalized = path.lstrip("/")
        return self.mkdir(normalized, exist_ok=True)

    def __repr__(self):
        return f"<AzureBlobFilesystem(account={self.account_name})>"


# ----------------------------
# Experiment and timing config
# ----------------------------
_TRAINING_TIME_THRESHOLD = 600
_PREDICTION_TIME_THRESHOLD = 450

storage_account_name = "azurefilestorageaccount"

_EXPERIMENT_PARAMS = {
    "smoke_test": {
        "data": (
            "https://air-example-data-2.s3.us-west-2.amazonaws.com/"
            "10G-xgboost-data.parquet/8034b2644a1d426d9be3bbfa78673dfa_000000.parquet"
        ),
        "num_workers": 2,
        "cpus_per_worker": 1,
    },
    "10G": {
        # Azure Blob dataset
        "data": "az://traineddata/testdata/10G-xgboost-data/",
        "num_workers": 3,
        "cpus_per_worker": 6,
    },
    "100G": {
        #  Azure Blob dataset
        "data": "az://traineddata/testdata/100G-xgboost-data/",
        "num_workers": 10,
        "cpus_per_worker": 12,
    },
}

# ----------------------------
# Predictors
# ----------------------------
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

# ----------------------------
# Train loop functions
# ----------------------------
def xgboost_train_loop_function(config: Dict):
    # 1. Get the dataset shard for the worker and convert to a `xgboost.DMatrix`
    train_ds_iter = ray.train.get_dataset_shard("train")
    train_df = train_ds_iter.materialize().to_pandas()

    label_column, params = config["label_column"], config["params"]
    train_X, train_y = train_df.drop(label_column, axis=1), train_df[label_column]

    dtrain = xgb.DMatrix(train_X, label=train_y)

    # 2. Do distributed data-parallel training.
    # Ray Train sets up the necessary coordinator processes and
    # environment variables for your workers to communicate with each other.
    report_callback = config["report_callback_cls"]
    xgb.train(
        params,
        dtrain=dtrain,
        num_boost_round=10,
        callbacks=[report_callback()],
    )


def lightgbm_train_loop_function(config: Dict):
    # 1. Get the dataset shard for the worker and convert to a DataFrame
    train_ds_iter = ray.train.get_dataset_shard("train")
    train_df = train_ds_iter.materialize().to_pandas()

    label_column, params = config["label_column"], config["params"]
    train_X, train_y = train_df.drop(label_column, axis=1), train_df[label_column]
    train_set = lgb.Dataset(train_X, label=train_y)

    # 2. Do distributed data-parallel training.
    # Ray Train sets up the necessary coordinator processes and
    # environment variables for your workers to communicate with each other.
    report_callback = config["report_callback_cls"]
    lgb.train(
        params,
        train_set=train_set,
        num_boost_round=10,
        callbacks=[report_callback()],
    )


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


def train(
    framework: str,
    data_path: str,
    num_workers: int,
    cpus_per_worker: int,
    storage_interface: str,
) -> ray.train.Result:
    fs = get_filesystem(storage_interface)
    ds = data.read_parquet(data_path, filesystem=fs)
    framework_params = _FRAMEWORK_PARAMS[framework]

    trainer_cls = framework_params["trainer_cls"]
    framework_train_loop_fn = framework_params["train_loop_function"]

    trainer = trainer_cls(
        scaling_config=ScalingConfig(
            num_workers=num_workers,
            resources_per_worker={"CPU": cpus_per_worker},
        ),
        datasets={"train": ds},
        # Since RunConfig expects storage_filesystem to be a pyarrow.fs.FileSystem
        # instance, we are providing the same
        run_config=RunConfig(
            storage_path="traineddata/cluster_storage",
            name=f"{framework}_benchmark",
            storage_filesystem= fs if storage_interface == "pyarrow" else RayAzureFSAdapter(fs),
        ),
        params=framework_params["train_loop_config"]["params"],
        label_column=framework_params["train_loop_config"]["label_column"],
    )
    result = trainer.fit()
    return result

def get_filesystem(storage_interface: str):
    if storage_interface == "pyarrow":
        fs = pyarrow.fs.AzureFileSystem(account_name=storage_account_name)
    elif storage_interface == "adlfs":
        import adlfs
        from azure.identity import DefaultAzureCredential
        credential = DefaultAzureCredential()
        fs = adlfs.AzureBlobFileSystem(account_name=storage_account_name, credential=credential)
    else:
        raise ValueError(f"Unsupported storage interface: {storage_interface}")
    return fs

def predict(
    framework: str,
    result: ray.train.Result,
    data_path: str,
    storage_interface: str,
):
    framework_params = _FRAMEWORK_PARAMS[framework]

    predictor_cls = framework_params["predictor_cls"]
    fs = get_filesystem(storage_interface)
    ds = data.read_parquet(data_path, filesystem=fs)
    ds = ds.drop_columns(["labels"])

    concurrency = int(ray.cluster_resources()["CPU"] // 2)
    ds.map_batches(
        predictor_cls,
        # Improve prediction throughput with larger batch size than default 4096
        batch_size=8192,
        concurrency=concurrency,
        fn_constructor_kwargs={
            "report_callback_cls": framework_params["train_loop_config"][
                "report_callback_cls"
            ],
            "result": result,
        },
        batch_format="pandas",
    ).write_parquet("traineddata/cluster_storage/predictions", filesystem=fs)


def main(args):
    framework = args.framework

    experiment = args.size if not args.smoke_test else "smoke_test"
    experiment_params = _EXPERIMENT_PARAMS[experiment]

    data_path, num_workers, cpus_per_worker = (
        experiment_params["data"],
        experiment_params["num_workers"],
        experiment_params["cpus_per_worker"],
    )
    if args.num_workers is not None:
        num_workers = args.num_workers
    if args.cpus_per_worker is not None:
        cpus_per_worker = args.cpus_per_worker

    print(f"Running {framework} training benchmark...")
    training_start = time.perf_counter()
    result = train(framework, data_path, num_workers, cpus_per_worker, args.storage_interface)
    training_time = time.perf_counter() - training_start

    print(f"Running {framework} prediction benchmark...")
    prediction_start = time.perf_counter()
    predict(framework, result, data_path, args.storage_interface)
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
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "framework", type=str, choices=["xgboost", "lightgbm"], default="xgboost"
    )
    parser.add_argument("--size", type=str, choices=["10G", "100G"], default="10G")
    parser.add_argument(
        "--account-name",
        type=str,
        required=True,
        help="Azure storage account name (for adlfs)"
    )
    parser.add_argument(
        "--storage-interface",
        type=str,
        choices=["pyarrow", "adlfs"],
        default="pyarrow"
    )
    parser.add_argument(
        "--num-workers",
        type=int,
        default=8,
        help="Number of workers."
    )
    parser.add_argument(
        "--cpus-per-worker",
        type=int,
        default=4,
        help="Number of CPUs per worker."
    )
    # Add a flag for disabling the timeout error.
    # Use case: running the benchmark as a documented example, in infra settings
    # different from the formal benchmark's EC2 setup.
    parser.add_argument(
        "--disable-check",
        action="store_true",
        help="disable runtime error on benchmark timeout",
    )
    parser.add_argument("--smoke-test", action="store_true")
    args = parser.parse_args()
    storage_account_name = args.account_name
    main(args)
