**How To Run Ray Model to access Azure Blob Directly**:
  There are two ways that can be used interact with Azure Blob directly in ray model:
  1. Using pyarrow interface (Recommended)
  2. Using adlfs interface

**adlfs and pyarrow have different design philosophies**:
<table>
  <tr>
    <th> Library </th>
    <th> Type </th>
    <th> Interface </th>
  </tr>
  <tr>
    <td> adlfs.AzureBlobFileSystem </td>
    <td> High-level Pythonic wrapper </td>
    <td> Built on Azure SDK for Python </td>
  </tr>
  <tr>
    <td> pyarrow.fs.AzureFileSystem </td>
    <td> Low-level C++ binding (Arrow native FS) </td>
    <td> Conforms to Arrow’s FileSystem API (open_input_file, open_output_stream) </td>
  </tr>
</table>

**Using Pyarrow Interface**:
  This method uses native [pyarrow.AzureFileSystem](https://arrow.apache.org/docs/python/generated/pyarrow.fs.AzureFileSystem.html) implementation to interact with Azure Blob storage.

  Below is the output where pyarrow has been used for inference model:
  Usage:
  ```sh
  git clone https://github.com/Azure-Samples/aks-ray-sample.git
  cd aks-ray-sample/sample-tuning-setup/direct-blob-access/
  ./deploy.sh --sa <StorageAccountName> --sa-rg <StorageAccount_ResourceGroup>
  ```
  Note: If the script encounters permission issues, please grant the required role and then type `ok` to continue.

  Output:

  ```sh
  2025-10-14 07:19:57,424 - INFO - Note: NumExpr detected 16 cores but "NUMEXPR_MAX_THREADS" not set, so enforcing safe limit of 8.
  2025-10-14 07:19:57,424 - INFO - NumExpr defaulting to 8 threads.
  2025-10-14 07:19:58,005 INFO cli.py:41 -- Job submission server address: http://rayjob-tune-gpt2-m6ctg-head-svc.kuberay.svc.cluster.local:8265
  2025-10-14 07:19:59,107 SUCC cli.py:65 -- ---------------------------------------------------
  2025-10-14 07:19:59,108 SUCC cli.py:66 -- Job 'rayjob-tune-gpt2-bh9hz' submitted successfully
  2025-10-14 07:19:59,108 SUCC cli.py:67 -- ---------------------------------------------------
  2025-10-14 07:19:59,108 INFO cli.py:291 -- Next steps
  2025-10-14 07:19:59,108 INFO cli.py:292 -- Query the logs of the job:
  2025-10-14 07:19:59,108 INFO cli.py:294 -- ray job logs rayjob-tune-gpt2-bh9hz
  2025-10-14 07:19:59,108 INFO cli.py:296 -- Query the status of the job:
  2025-10-14 07:19:59,108 INFO cli.py:298 -- ray job status rayjob-tune-gpt2-bh9hz
  2025-10-14 07:19:59,108 INFO cli.py:300 -- Request the job to be stopped:
  2025-10-14 07:19:59,108 INFO cli.py:302 -- ray job stop rayjob-tune-gpt2-bh9hz
  2025-10-14 07:20:00,444 - INFO - Note: NumExpr detected 16 cores but "NUMEXPR_MAX_THREADS" not set, so enforcing safe limit of 8.
  2025-10-14 07:20:00,444 - INFO - NumExpr defaulting to 8 threads.
  2025-10-14 07:20:01,030 INFO cli.py:41 -- Job submission server address: http://rayjob-tune-gpt2-m6ctg-head-svc.kuberay.svc.cluster.local:8265
  2025-10-14 07:19:58,862 INFO job_manager.py:568 -- Runtime env is setting up.
  Running xgboost training benchmark...
  2025-10-14 07:20:10,603 INFO worker.py:1692 -- Using address 10.244.3.40:6379 set in the environment variable RAY_ADDRESS
  2025-10-14 07:20:10,606 INFO worker.py:1833 -- Connecting to existing Ray cluster at address: 10.244.3.40:6379...
  2025-10-14 07:20:10,621 INFO worker.py:2004 -- Connected to Ray cluster. View the dashboard at 10.244.3.40:8265
  /home/ray/anaconda3/lib/python3.10/site-packages/ray/_private/worker.py:2052: FutureWarning: Tip: In future versions of Ray, Ray will no longer override accelerator visible devices env var if num_gpus=0 or num_gpus=None (default). To enable this behavior and turn off this error message, set RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO=0
    warnings.warn(
  
  Parquet dataset sampling 0:   0%|          | 0.00/3.00 [00:00<?, ? file/s]
  Parquet dataset sampling 0:   0%|          | 0.00/3.00 [00:01<?, ? file/s]
  Parquet dataset sampling 0:   0%|          | 0.00/3.00 [00:02<?, ? file/s]
  Parquet dataset sampling 0:   0%|          | 0.00/3.00 [00:03<?, ? file/s]
  Parquet dataset sampling 0:   0%|          | 0.00/3.00 [00:04<?, ? file/s]
  Parquet dataset sampling 0:   0%|          | 0.00/3.00 [00:05<?, ? file/s]
  Parquet dataset sampling 0:   0%|          | 0.00/3.00 [00:06<?, ? file/s]
  Parquet dataset sampling 0:   0%|          | 0.00/3.00 [00:07<?, ? file/s]
  Parquet dataset sampling 0:   0%|          | 0.00/3.00 [00:08<?, ? file/s]
  Parquet dataset sampling 0:   0%|          | 0.00/3.00 [00:09<?, ? file/s]
  Parquet dataset sampling 0:   0%|          | 0.00/3.00 [00:10<?, ? file/s]
  Parquet dataset sampling 0:   0%|          | 0.00/3.00 [00:11<?, ? file/s]
  Parquet dataset sampling 0:  33%|███▎      | 1.00/3.00 [00:12<00:24, 12.0s/ file]
  Parquet dataset sampling 0:  33%|███▎      | 1.00/3.00 [00:12<00:24, 12.0s/ file]
  Parquet dataset sampling 0:  33%|███▎      | 1.00/3.00 [00:13<00:24, 12.0s/ file]
  Parquet dataset sampling 0:  67%|██████▋   | 2.00/3.00 [00:14<00:06, 6.14s/ file]
  Parquet dataset sampling 0:  67%|██████▋   | 2.00/3.00 [00:14<00:06, 6.14s/ file]
  Parquet dataset sampling 0:  67%|██████▋   | 2.00/3.00 [00:15<00:06, 6.14s/ file]
  Parquet dataset sampling 0:  67%|██████▋   | 2.00/3.00 [00:16<00:06, 6.14s/ file]
  Parquet dataset sampling 0:  67%|██████▋   | 2.00/3.00 [00:17<00:06, 6.14s/ file]
  Parquet dataset sampling 0:  67%|██████▋   | 2.00/3.00 [00:18<00:06, 6.14s/ file]
  Parquet dataset sampling 0:  67%|██████▋   | 2.00/3.00 [00:19<00:06, 6.14s/ file]
  Parquet dataset sampling 0:  67%|██████▋   | 2.00/3.00 [00:20<00:06, 6.14s/ file]
  Parquet dataset sampling 0: 100%|██████████| 3.00/3.00 [00:20<00:00, 6.44s/ file]
  Parquet dataset sampling 0: 100%|██████████| 3.00/3.00 [00:20<00:00, 6.44s/ file]
  
  
  Parquet dataset sampling 0: 100%|██████████| 3.00/3.00 [00:20<00:00, 6.95s/ file]
  2025-10-14 07:20:31,549 INFO parquet_datasource.py:699 -- Estimated parquet encoding ratio is 0.850.
  2025-10-14 07:20:31,549 INFO parquet_datasource.py:759 -- Estimated parquet reader batch size at 383480 rows
  /home/ray/anaconda3/lib/python3.10/site-packages/google/rpc/__init__.py:18: UserWarning: pkg_resources is deprecated as an API. See https://setuptools.pypa.io/en/latest/pkg_resources.html. The pkg_resources package is slated for removal as early as 2025-11-30. Refrain from using this package or pin to Setuptools<81.
    import pkg_resources
  
  View detailed results here: traineddata/cluster_storage/xgboost_benchmark
  To visualize your results with TensorBoard, run: `tensorboard --logdir /tmp/ray/session_2025-10-14_07-18-54_398953_1/artifacts/2025-10-14_07-20-44/xgboost_benchmark/driver_artifacts`
  
  Training started without custom configuration.
  (XGBoostTrainer pid=2433) Started distributed worker processes:
  (XGBoostTrainer pid=2433) - (node_id=49e7cbe9b2879707a12db2442aeb3ffa615687827d77de8f2690047d, ip=10.244.3.40, pid=2521) world_rank=0, local_rank=0, node_rank=0
  (XGBoostTrainer pid=2433) - (node_id=49e7cbe9b2879707a12db2442aeb3ffa615687827d77de8f2690047d, ip=10.244.3.40, pid=2522) world_rank=1, local_rank=1, node_rank=0
  (XGBoostTrainer pid=2433) - (node_id=49e7cbe9b2879707a12db2442aeb3ffa615687827d77de8f2690047d, ip=10.244.3.40, pid=2523) world_rank=2, local_rank=2, node_rank=0
  (XGBoostTrainer pid=2433) - (node_id=713d04f45d2c9de732da15fb65bbfbcbcc011a40c429728c8eddeb2a, ip=10.244.0.230, pid=658) world_rank=3, local_rank=0, node_rank=1
  (XGBoostTrainer pid=2433) - (node_id=713d04f45d2c9de732da15fb65bbfbcbcc011a40c429728c8eddeb2a, ip=10.244.0.230, pid=659) world_rank=4, local_rank=1, node_rank=1
  (XGBoostTrainer pid=2433) - (node_id=abf046a3b921d1579e0f2d60bb1862f8871786a6c61dcca0a166ca69, ip=10.244.1.106, pid=416) world_rank=5, local_rank=0, node_rank=2
  (XGBoostTrainer pid=2433) - (node_id=abf046a3b921d1579e0f2d60bb1862f8871786a6c61dcca0a166ca69, ip=10.244.1.106, pid=415) world_rank=6, local_rank=1, node_rank=2
  (XGBoostTrainer pid=2433) - (node_id=abf046a3b921d1579e0f2d60bb1862f8871786a6c61dcca0a166ca69, ip=10.244.1.106, pid=417) world_rank=7, local_rank=2, node_rank=2
  (RayTrainWorker pid=416, ip=10.244.1.106) [07:20:51] Task [xgboost.ray-rank=00000005]:fec1106d48ac463dd062482e02000000 got rank 5
  (SplitCoordinator pid=2719) Registered dataset logger for dataset train_1_0
  (SplitCoordinator pid=2719) Starting execution of Dataset train_1_0. Full logs are in /tmp/ray/session_2025-10-14_07-18-54_398953_1/logs/ray-data
  (SplitCoordinator pid=2719) Execution plan of Dataset train_1_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ReadParquet] -> OutputSplitter[split(8, equal=True)]
  
  (pid=2719) Running 0: 0.00 row [00:00, ? row/s]
  
  (SplitCoordinator pid=2719) ⚠️  Ray's object store is configured to use only 29.6% of available memory (79.9GiB out of 270.0GiB total). For optimal Ray Data performance, we recommend setting the object store to at least 50% of available memory. You can do this by setting the 'object_store_memory' parameter when calling ray.init() or by setting the RAY_DEFAULT_OBJECT_STORE_MEMORY_PROPORTION environment variable.
  
  (pid=2719) Running 0: 0.00 row [00:00, ? row/s]
  
  (pid=2719) - ReadParquet 1: 0.00 row [00:00, ? row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 9.2GB/39.9GB object store: : 0.00 row [00:01, ? row/s] - split(8, equal=True) 2: 0.00 row [00:00, ? row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 9.2GB/39.9GB object store: : 0.00 row [00:01, ? row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 163; Resources: 37.0 CPU, 9.2GB object
  store: : 0.00 row [00:01, ? row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 163; Resources: 37.0 CPU, 9.2GB object
  store: : 0.00 row [00:01, ? row/s]
  
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 0.0B object store; [all object
  s local]: : 0.00 row [00:01, ? row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 9.2GB/39.9GB object store: : 0.00 row [00:02, ? row/s]: 0.00 row [00:01, ? row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 163; Resources: 37.0 CPU, 9.2GB object
  store: : 0.00 row [00:02, ? row/s]
  
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 9.2GB/39.9GB object store: : 0.00 row [00:02, ? row/s]: 0.00 row [00:02, ? row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 163; Resources: 37.0 CPU, 9.2GB object
  store: : 0.00 row [00:02, ? row/s]
  
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 9.2GB/39.9GB object store: : 0.00 row [00:04, ? row/s]: 0.00 row [00:02, ? row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 163; Resources: 37.0 CPU, 9.2GB object
  store: : 0.00 row [00:04, ? row/s]
  
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 9.2GB/39.9GB object store: : 0.00 row [00:05, ? row/s]: 0.00 row [00:04, ? row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 163; Resources: 37.0 CPU, 9.2GB object
  store: : 0.00 row [00:05, ? row/s]
  
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 9.2GB/39.9GB object store: : 0.00 row [00:06, ? row/s]: 0.00 row [00:05, ? row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 163; Resources: 37.0 CPU, 9.2GB object
  store: : 0.00 row [00:06, ? row/s]
  
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 9.2GB/39.9GB object store: : 0.00 row [00:07, ? row/s]: 0.00 row [00:06, ? row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 163; Resources: 37.0 CPU, 9.2GB object
  store: : 0.00 row [00:07, ? row/s]
  
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 9.2GB/39.9GB object store: : 0.00 row [00:08, ? row/s]: 0.00 row [00:07, ? row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 163; Resources: 37.0 CPU, 9.2GB object
  store: : 0.00 row [00:08, ? row/s]
  
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 9.2GB/39.9GB object store: : 0.00 row [00:09, ? row/s]: 0.00 row [00:08, ? row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 163; Resources: 37.0 CPU, 9.2GB object
  store: : 0.00 row [00:09, ? row/s]
  
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 9.2GB/39.9GB object store: : 0.00 row [00:10, ? row/s]: 0.00 row [00:09, ? row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 163; Resources: 37.0 CPU, 9.2GB object
  store: : 0.00 row [00:10, ? row/s]
  
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 9.2GB/39.9GB object store: : 0.00 row [00:11, ? row/s]: 0.00 row [00:10, ? row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 163; Resources: 37.0 CPU, 9.2GB object
  store: : 0.00 row [00:11, ? row/s]
  
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 9.2GB/39.9GB object store: : 0.00 row [00:12, ? row/s]: 0.00 row [00:11, ? row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 163; Resources: 37.0 CPU, 9.2GB object
  store: : 0.00 row [00:12, ? row/s]
  
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 9.2GB/39.9GB object store: : 0.00 row [00:13, ? row/s]: 0.00 row [00:12, ? row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 163; Resources: 37.0 CPU, 9.2GB object
  store: : 0.00 row [00:13, ? row/s]
  
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 9.2GB/39.9GB object store: : 0.00 row [00:14, ? row/s]: 0.00 row [00:13, ? row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 163; Resources: 37.0 CPU, 9.2GB object
  store: : 0.00 row [00:14, ? row/s]
  
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 9.2GB/39.9GB object store: : 0.00 row [00:15, ? row/s]: 0.00 row [00:14, ? row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 163; Resources: 37.0 CPU, 9.2GB object
  store: : 0.00 row [00:15, ? row/s]
  
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.5GB/39.9GB object store: : 0.00 row [00:16, ? row/s]: 0.00 row [00:15, ? row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.5GB/39.9GB object store: : 0.00 row [00:16, ? row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 161; Resources: 37.0 CPU, 2.5GB object
  store: : 0.00 row [00:16, ? row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 161; Resources: 37.0 CPU, 2.5GB object
  store:   0%|          | 0.00/40.0M [00:16<?, ? row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 161; Resources: 37.0 CPU, 2.5GB object
  store:   2%|▏         | 600k/40.0M [00:16<17:52, 36.7k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 161; Resources: 37.0 CPU, 2.5GB object
  store:   2%|▏         | 600k/40.0M [00:16<17:52, 36.7k row/s]
  
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 3; Resources: 0.0 CPU, 0.0B object store; [all object
  s local]: : 0.00 row [00:16, ? row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 36/37 CPU, 2.9GB/39.9GB object store: : 0.00 row [00:17, ? row/s]: 0.00 row [00:16, ? row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 36/37 CPU, 2.9GB/39.9GB object store: : 0.00 row [00:17, ? row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 152; Resources: 37.0 CPU, 3.1GB object
  store:   2%|▏         | 600k/40.0M [00:17<17:52, 36.7k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 152; Resources: 37.0 CPU, 3.1GB object
  store:   6%|▌         | 2.40M/40.0M [00:17<03:30, 178k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 152; Resources: 37.0 CPU, 3.1GB object
  store:   6%|▌         | 2.40M/40.0M [00:17<03:30, 178k row/s]
  
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 0.0B object store; [all objec
  ts local]: : 0.00 row [00:17, ? row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 36/37 CPU, 3.6GB/39.9GB object store: : 0.00 row [00:18, ? row/s] : 0.00 row [00:17, ? row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 36/37 CPU, 3.6GB/39.9GB object store:   0%|          | 0.00/40.0M [00:18<?, ? row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 36/37 CPU, 3.6GB/39.9GB object store:   4%|▍         | 1.80M/40.0M [00:18<06:32, 97.4k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 36/37 CPU, 3.6GB/39.9GB object store:   4%|▍         | 1.80M/40.0M [00:18<06:32, 97.4k row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 137; Resources: 37.0 CPU, 3.4GB object
  store:   6%|▌         | 2.40M/40.0M [00:18<03:30, 178k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 137; Resources: 37.0 CPU, 3.4GB object
  store:  14%|█▎        | 5.40M/40.0M [00:18<01:14, 462k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 137; Resources: 37.0 CPU, 3.4GB object
  store:  14%|█▎        | 5.40M/40.0M [00:18<01:14, 462k row/s]
  
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 66.6MB object store; [5/11 ob
  jects local]: : 0.00 row [00:18, ? row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 66.6MB object store; [5/11 ob
  jects local]:   0%|          | 0.00/40.0M [00:18<?, ? row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 66.6MB object store; [5/11 ob
  jects local]:   6%|▌         | 2.20M/40.0M [00:18<05:17, 119k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 36/37 CPU, 3.4GB/39.9GB object store:   4%|▍         | 1.80M/40.0M [00:19<06:32, 97.4k row/s]0.0M [00:18<05:17, 119k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 36/37 CPU, 3.4GB/39.9GB object store:  10%|█         | 4.00M/40.0M [00:19<02:25, 248k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 36/37 CPU, 3.4GB/39.9GB object store:  10%|█         | 4.00M/40.0M [00:19<02:25, 248k row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 126; Resources: 37.0 CPU, 3.4GB object
  store:  14%|█▎        | 5.40M/40.0M [00:19<01:14, 462k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 126; Resources: 37.0 CPU, 3.4GB object
  store:  18%|█▊        | 7.40M/40.0M [00:19<00:49, 655k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 126; Resources: 37.0 CPU, 3.4GB object
  store:  18%|█▊        | 7.40M/40.0M [00:19<00:49, 655k row/s]
  
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 0.0B object store; [6/22 obje
  cts local]:   6%|▌         | 2.20M/40.0M [00:19<05:17, 119k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 0.0B object store; [6/22 obje
  cts local]:  11%|█         | 4.40M/40.0M [00:19<02:12, 268k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 3.4GB/39.9GB object store:  10%|█         | 4.00M/40.0M [00:20<02:25, 248k row/s].0M [00:19<02:12, 268k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 3.4GB/39.9GB object store:  11%|█         | 4.40M/40.0M [00:20<02:16, 261k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 3.4GB/39.9GB object store:  11%|█         | 4.40M/40.0M [00:20<02:16, 261k row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 126; Resources: 37.0 CPU, 3.4GB object
  store:  18%|█▊        | 7.40M/40.0M [00:20<00:49, 655k row/s]
  
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 3.4GB/39.9GB object store:  11%|█         | 4.40M/40.0M [00:21<02:16, 261k row/s].0M [00:20<02:12, 268k row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 126; Resources: 37.0 CPU, 3.4GB object
  store:  18%|█▊        | 7.40M/40.0M [00:21<00:49, 655k row/s]
  
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 3.4GB/39.9GB object store:  11%|█         | 4.40M/40.0M [00:22<02:16, 261k row/s].0M [00:21<02:12, 268k row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 126; Resources: 37.0 CPU, 3.4GB object
  store:  18%|█▊        | 7.40M/40.0M [00:22<00:49, 655k row/s]
  
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 3.4GB/39.9GB object store:  11%|█         | 4.40M/40.0M [00:23<02:16, 261k row/s].0M [00:22<02:12, 268k row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 126; Resources: 37.0 CPU, 3.4GB object
  store:  18%|█▊        | 7.40M/40.0M [00:23<00:49, 655k row/s]
  
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 35/37 CPU, 3.1GB/39.9GB object store:  11%|█         | 4.40M/40.0M [00:24<02:16, 261k row/s].0M [00:23<02:12, 268k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 35/37 CPU, 3.1GB/39.9GB object store:  12%|█▏        | 4.40M/38.0M [00:24<02:08, 261k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 35/37 CPU, 3.1GB/39.9GB object store:  13%|█▎        | 5.00M/38.0M [00:24<02:29, 221k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 35/37 CPU, 3.1GB/39.9GB object store:  13%|█▎        | 5.00M/38.0M [00:24<02:29, 221k row/s]
  
  
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 266.6MB object store; [6/30 o
  bjects local]:  11%|█         | 4.40M/40.0M [00:24<02:12, 268k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 266.6MB object store; [6/30 o
  bjects local]:  12%|█▏        | 4.40M/36.4M [00:24<01:59, 268k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 266.6MB object store; [6/30 o
  bjects local]:  16%|█▌        | 5.80M/36.4M [00:24<01:53, 269k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 266.6MB object store; [6/30 o
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 118; Resources: 37.0 CPU, 2.9GB object
  store:  18%|█▊        | 7.40M/40.0M [00:24<00:49, 655k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 118; Resources: 37.0 CPU, 2.9GB object
  store:  20%|██        | 7.40M/36.4M [00:24<00:44, 655k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 118; Resources: 37.0 CPU, 2.9GB object
  store:  23%|██▎       | 8.20M/36.4M [00:24<01:08, 415k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 118; Resources: 37.0 CPU, 2.9GB object
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.6GB/39.9GB object store:  13%|█▎        | 5.00M/38.0M [00:25<02:29, 221k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.6GB/39.9GB object store:  14%|█▍        | 5.00M/34.8M [00:25<02:15, 221k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.6GB/39.9GB object store:  20%|██        | 7.00M/34.8M [00:25<01:07, 413k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.6GB/39.9GB object store:  20%|██        | 7.00M/34.8M [00:25<01:07, 413k row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 113; Resources: 37.0 CPU, 2.6GB object
  store:  23%|██▎       | 8.20M/36.4M [00:25<01:08, 415k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 113; Resources: 37.0 CPU, 2.6GB object
  store:  24%|██▎       | 8.20M/34.8M [00:25<01:04, 415k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 113; Resources: 37.0 CPU, 2.6GB object
  store:  25%|██▌       | 8.70M/34.8M [00:25<01:01, 424k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 113; Resources: 37.0 CPU, 2.6GB object
  store:  25%|██▌       | 8.70M/34.8M [00:25<01:01, 424k row/s]
  
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 0.0B object store; [6/35 obje
    cts local]:  16%|█▌        | 5.80M/36.4M [00:25<01:53, 269k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 0.0B object store; [6/35 obje
  cts local]:  17%|█▋        | 5.80M/34.8M [00:25<01:47, 269k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 0.0B object store; [6/35 obje
  cts local]:  20%|██        | 7.00M/34.8M [00:25<01:19, 350k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.6GB/39.9GB object store:  20%|██        | 7.00M/34.8M [00:26<01:07, 413k row/s].8M [00:25<01:19, 350k row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 113; Resources: 37.0 CPU, 2.6GB object
  store:  25%|██▌       | 8.70M/34.8M [00:26<01:01, 424k row/s]
  
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 36/37 CPU, 2.6GB/39.9GB object store:  20%|██        | 7.00M/34.8M [00:27<01:07, 413k row/s].8M [00:26<01:19, 350k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 36/37 CPU, 2.6GB/39.9GB object store:  20%|██        | 7.00M/34.6M [00:27<01:06, 413k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 36/37 CPU, 2.6GB/39.9GB object store:  21%|██▏       | 7.40M/34.6M [00:27<01:15, 361k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 36/37 CPU, 2.6GB/39.9GB object store:  21%|██▏       | 7.40M/34.6M [00:27<01:15, 361k row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 109; Resources: 37.0 CPU, 2.7GB object
  store:  25%|██▌       | 8.70M/34.8M [00:27<01:01, 424k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 109; Resources: 37.0 CPU, 2.7GB object
  store:  25%|██▍       | 8.70M/34.9M [00:27<01:01, 424k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 109; Resources: 37.0 CPU, 2.7GB object
  store:  27%|██▋       | 9.60M/34.9M [00:27<00:59, 427k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 109; Resources: 37.0 CPU, 2.7GB object
  store:  27%|██▋       | 9.60M/34.9M [00:27<00:59, 427k row/s]
  
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 66.6MB object store; [6/39 ob
  jects local]:  20%|██        | 7.00M/34.8M [00:27<01:19, 350k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 66.6MB object store; [6/39 ob
  jects local]:  20%|██        | 7.00M/34.9M [00:27<01:19, 350k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 66.6MB object store; [6/39 ob
  jects local]:  22%|██▏       | 7.60M/34.9M [00:27<01:20, 338k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.9GB/39.9GB object store:  21%|██▏       | 7.40M/34.6M [00:28<01:15, 361k row/s]34.9M [00:27<01:20, 338k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.9GB/39.9GB object store:  21%|██        | 7.40M/35.4M [00:28<01:17, 361k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.9GB/39.9GB object store:  23%|██▎       | 8.30M/35.4M [00:28<01:02, 436k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.9GB/39.9GB object store:  23%|██▎       | 8.30M/35.4M [00:28<01:02, 436k row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 101; Resources: 37.0 CPU, 2.9GB object
  store:  27%|██▋       | 9.60M/34.9M [00:28<00:59, 427k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 101; Resources: 37.0 CPU, 2.9GB object
  store:  27%|██▋       | 9.60M/35.6M [00:28<01:00, 427k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 101; Resources: 37.0 CPU, 2.9GB object
  store:  31%|███▏      | 11.2M/35.6M [00:28<00:40, 602k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 101; Resources: 37.0 CPU, 2.9GB object
  store:  31%|███▏      | 11.2M/35.6M [00:28<00:40, 602k row/s]
  
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 66.6MB object store; [8/47 ob
  jects local]:  22%|██▏       | 7.60M/34.9M [00:28<01:20, 338k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 66.6MB object store; [8/47 ob
  jects local]:  21%|██▏       | 7.60M/35.6M [00:28<01:22, 338k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 66.6MB object store; [8/47 ob
  jects local]:  24%|██▍       | 8.60M/35.6M [00:28<01:04, 419k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 3.2GB/39.9GB object store:  23%|██▎       | 8.30M/35.4M [00:29<01:02, 436k row/s]35.6M [00:28<01:04, 419k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 3.2GB/39.9GB object store:  23%|██▎       | 8.30M/35.9M [00:29<01:03, 436k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 3.2GB/39.9GB object store:  28%|██▊       | 10.2M/35.9M [00:29<00:38, 671k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 3.2GB/39.9GB object store:  28%|██▊       | 10.2M/35.9M [00:29<00:38, 671k row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 88; Resources: 37.0 CPU, 3.1GB object s
  tore:  31%|███▏      | 11.2M/35.6M [00:29<00:40, 602k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 88; Resources: 37.0 CPU, 3.1GB object s
  tore:  32%|███▏      | 11.2M/35.5M [00:29<00:40, 602k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 88; Resources: 37.0 CPU, 3.1GB object s
  tore:  38%|███▊      | 13.5M/35.5M [00:29<00:24, 883k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 88; Resources: 37.0 CPU, 3.1GB object s
  tore:  38%|███▊      | 13.5M/35.5M [00:29<00:24, 883k row/s]
  
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 66.6MB object store; [13/60 o
  bjects local]:  24%|██▍       | 8.60M/35.6M [00:29<01:04, 419k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 66.6MB object store; [13/60 o
  bjects local]:  24%|██▍       | 8.60M/35.5M [00:29<01:04, 419k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 66.6MB object store; [13/60 o
  bjects local]:  30%|██▉       | 10.6M/35.5M [00:29<00:38, 644k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.8GB/39.9GB object store:  28%|██▊       | 10.2M/35.9M [00:30<00:38, 671k row/s]/35.5M [00:29<00:38, 644k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.8GB/39.9GB object store:  30%|██▉       | 10.2M/34.6M [00:30<00:36, 671k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.8GB/39.9GB object store:  34%|███▍      | 11.8M/34.6M [00:30<00:26, 845k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.8GB/39.9GB object store:  34%|███▍      | 11.8M/34.6M [00:30<00:26, 845k row/s]
  
  
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 66.6MB object store; [14/67 o
  bjects local]:  30%|██▉       | 10.6M/35.5M [00:30<00:38, 644k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 66.6MB object store; [14/67 o
  bjects local]:  31%|███       | 10.6M/34.4M [00:30<00:36, 644k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 66.6MB object store; [14/67 o
  bjects local]:  35%|███▍      | 12.0M/34.4M [00:30<00:29, 772k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 66.6MB object store; [14/67 o
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 81; Resources: 37.0 CPU, 2.8GB object s
  tore:  38%|███▊      | 13.5M/35.5M [00:30<00:24, 883k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 81; Resources: 37.0 CPU, 2.8GB object s
  tore:  39%|███▉      | 13.5M/34.4M [00:30<00:23, 883k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 81; Resources: 37.0 CPU, 2.8GB object s
  tore:  41%|████      | 14.1M/34.4M [00:30<00:24, 823k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 81; Resources: 37.0 CPU, 2.8GB object s
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.6GB/39.9GB object store:  34%|███▍      | 11.8M/34.6M [00:31<00:26, 845k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.6GB/39.9GB object store:  35%|███▍      | 11.8M/33.7M [00:31<00:25, 845k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.6GB/39.9GB object store:  38%|███▊      | 12.8M/33.7M [00:31<00:23, 873k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.6GB/39.9GB object store:  38%|███▊      | 12.8M/33.7M [00:31<00:23, 873k row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 77; Resources: 37.0 CPU, 2.6GB object s
  tore:  41%|████      | 14.1M/34.4M [00:31<00:24, 823k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 77; Resources: 37.0 CPU, 2.6GB object s
  tore:  42%|████▏     | 14.1M/33.7M [00:31<00:23, 823k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 77; Resources: 37.0 CPU, 2.6GB object s
  tore:  43%|████▎     | 14.5M/33.7M [00:31<00:26, 726k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 77; Resources: 37.0 CPU, 2.6GB object s
  tore:  43%|████▎     | 14.5M/33.7M [00:31<00:26, 726k row/s]
  
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 0.0B object store; [14/71 obj
  ects local]:  35%|███▍      | 12.0M/34.4M [00:31<00:29, 772k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 0.0B object store; [14/71 obj
  ects local]:  36%|███▌      | 12.0M/33.7M [00:31<00:28, 772k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 0.0B object store; [14/71 obj
  ects local]:  38%|███▊      | 12.8M/33.7M [00:31<00:27, 773k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 36/37 CPU, 2.6GB/39.9GB object store:  38%|███▊      | 12.8M/33.7M [00:32<00:23, 873k row/s]3.7M [00:31<00:27, 773k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 36/37 CPU, 2.6GB/39.9GB object store:  38%|███▊      | 12.8M/33.7M [00:32<00:23, 873k row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 76; Resources: 37.0 CPU, 2.5GB object s
  tore:  43%|████▎     | 14.5M/33.7M [00:32<00:26, 726k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 76; Resources: 37.0 CPU, 2.5GB object s
  tore:  43%|████▎     | 14.5M/33.6M [00:32<00:26, 726k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 76; Resources: 37.0 CPU, 2.5GB object s
  tore:  44%|████▎     | 14.6M/33.6M [00:32<00:33, 573k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 76; Resources: 37.0 CPU, 2.5GB object s
  tore:  44%|████▎     | 14.6M/33.6M [00:32<00:33, 573k row/s]
  
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 0.0B object store; [14/72 obj
  ects local]:  38%|███▊      | 12.8M/33.7M [00:32<00:27, 773k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 0.0B object store; [14/72 obj
  ects local]:  38%|███▊      | 12.8M/33.6M [00:32<00:26, 773k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 0.0B object store; [14/72 obj
  ects local]:  39%|███▊      | 13.0M/33.6M [00:32<00:31, 644k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.5GB/39.9GB object store:  38%|███▊      | 12.8M/33.7M [00:33<00:23, 873k row/s]3.6M [00:32<00:31, 644k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.5GB/39.9GB object store:  39%|███▊      | 12.8M/33.1M [00:33<00:23, 873k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.5GB/39.9GB object store:  40%|████      | 13.4M/33.1M [00:33<00:30, 650k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.5GB/39.9GB object store:  40%|████      | 13.4M/33.1M [00:33<00:30, 650k row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 71; Resources: 37.0 CPU, 2.5GB object s
  tore:  44%|████▎     | 14.6M/33.6M [00:33<00:33, 573k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 71; Resources: 37.0 CPU, 2.5GB object s
  tore:  45%|████▍     | 14.6M/32.6M [00:33<00:31, 573k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 71; Resources: 37.0 CPU, 2.5GB object s
  tore:  47%|████▋     | 15.2M/32.6M [00:33<00:30, 576k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 71; Resources: 37.0 CPU, 2.5GB object s
  tore:  47%|████▋     | 15.2M/32.6M [00:33<00:30, 576k row/s]
  
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 33.3MB object store; [16/77 o
  bjects local]:  39%|███▊      | 13.0M/33.6M [00:33<00:31, 644k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 33.3MB object store; [16/77 o
  bjects local]:  40%|███▉      | 13.0M/32.6M [00:33<00:30, 644k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 33.3MB object store; [16/77 o
  bjects local]:  42%|████▏     | 13.6M/32.6M [00:33<00:30, 629k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 36/37 CPU, 2.4GB/39.9GB object store:  40%|████      | 13.4M/33.1M [00:35<00:30, 650k row/s]/32.6M [00:33<00:30, 629k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 36/37 CPU, 2.4GB/39.9GB object store:  42%|████▏     | 13.4M/31.8M [00:35<00:28, 650k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 36/37 CPU, 2.4GB/39.9GB object store:  46%|████▌     | 14.5M/31.8M [00:35<00:23, 741k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 36/37 CPU, 2.4GB/39.9GB object store:  46%|████▌     | 14.5M/31.8M [00:35<00:23, 741k row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 57; Resources: 37.0 CPU, 2.4GB object s
  tore:  47%|████▋     | 15.2M/32.6M [00:35<00:30, 576k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 57; Resources: 37.0 CPU, 2.4GB object s
  tore:  49%|████▊     | 15.2M/31.2M [00:35<00:27, 576k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 57; Resources: 37.0 CPU, 2.4GB object s
  tore:  54%|█████▎    | 16.7M/31.2M [00:35<00:18, 793k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 57; Resources: 37.0 CPU, 2.4GB object s
  tore:  54%|█████▎    | 16.7M/31.2M [00:35<00:18, 793k row/s]
  
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 100.0MB object store; [22/91
  objects local]:  42%|████▏     | 13.6M/32.6M [00:35<00:30, 629k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 100.0MB object store; [22/91
  objects local]:  44%|████▎     | 13.6M/31.2M [00:35<00:27, 629k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 100.0MB object store; [22/91
  objects local]:  47%|████▋     | 14.8M/31.2M [00:35<00:21, 749k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.4GB/39.9GB object store:  46%|████▌     | 14.5M/31.8M [00:36<00:23, 741k row/s]M/31.2M [00:35<00:21, 749k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.4GB/39.9GB object store:  47%|████▋     | 14.5M/30.7M [00:36<00:21, 741k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.4GB/39.9GB object store:  51%|█████     | 15.6M/30.7M [00:36<00:18, 800k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.4GB/39.9GB object store:  51%|█████     | 15.6M/30.7M [00:36<00:18, 800k row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 50; Resources: 37.0 CPU, 2.4GB object s
  tore:  54%|█████▎    | 16.7M/31.2M [00:36<00:18, 793k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 50; Resources: 37.0 CPU, 2.4GB object s
  tore:  55%|█████▍    | 16.7M/30.6M [00:36<00:17, 793k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 50; Resources: 37.0 CPU, 2.4GB object s
  tore:  57%|█████▋    | 17.4M/30.6M [00:36<00:17, 763k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 50; Resources: 37.0 CPU, 2.4GB object s
  tore:  57%|█████▋    | 17.4M/30.6M [00:36<00:17, 763k row/s]
  
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 66.6MB object store; [22/99 o
  bjects local]:  47%|████▋     | 14.8M/31.2M [00:36<00:21, 749k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 66.6MB object store; [22/99 o
  bjects local]:  48%|████▊     | 14.8M/30.6M [00:36<00:21, 749k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 66.6MB object store; [22/99 o
  bjects local]:  51%|█████▏    | 15.7M/30.6M [00:36<00:19, 783k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 35/37 CPU, 2.3GB/39.9GB object store:  51%|█████     | 15.6M/30.7M [00:37<00:18, 800k row/s]/30.6M [00:36<00:19, 783k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 35/37 CPU, 2.3GB/39.9GB object store:  52%|█████▏    | 15.6M/30.1M [00:37<00:18, 800k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 35/37 CPU, 2.3GB/39.9GB object store:  55%|█████▍    | 16.4M/30.1M [00:37<00:17, 795k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 35/37 CPU, 2.3GB/39.9GB object store:  55%|█████▍    | 16.4M/30.1M [00:37<00:17, 795k row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 42; Resources: 37.0 CPU, 2.3GB object s
  tore:  57%|█████▋    | 17.4M/30.6M [00:37<00:17, 763k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 42; Resources: 37.0 CPU, 2.3GB object s
  tore:  58%|█████▊    | 17.4M/29.9M [00:37<00:16, 763k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 42; Resources: 37.0 CPU, 2.3GB object s
  tore:  61%|██████    | 18.1M/29.9M [00:37<00:15, 740k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 42; Resources: 37.0 CPU, 2.3GB object s
  tore:  61%|██████    | 18.1M/29.9M [00:37<00:15, 740k row/s]
  
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 0.0B object store; [22/106 ob
  jects local]:  51%|█████▏    | 15.7M/30.6M [00:37<00:19, 783k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 0.0B object store; [22/106 ob
  jects local]:  52%|█████▏    | 15.7M/29.9M [00:37<00:18, 783k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 0.0B object store; [22/106 ob
  jects local]:  55%|█████▌    | 16.6M/29.9M [00:37<00:16, 809k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.3GB/39.9GB object store:  55%|█████▍    | 16.4M/30.1M [00:38<00:17, 795k row/s]29.9M [00:37<00:16, 809k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.3GB/39.9GB object store:  55%|█████▍    | 16.4M/29.9M [00:38<00:17, 795k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.3GB/39.9GB object store:  55%|█████▌    | 16.6M/29.9M [00:38<00:20, 638k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.3GB/39.9GB object store:  55%|█████▌    | 16.6M/29.9M [00:38<00:20, 638k row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 41; Resources: 37.0 CPU, 2.3GB object s
  tore:  61%|██████    | 18.1M/29.9M [00:38<00:15, 740k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 41; Resources: 37.0 CPU, 2.3GB object s
  tore:  61%|██████    | 18.1M/29.8M [00:38<00:15, 740k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 41; Resources: 37.0 CPU, 2.3GB object s
  tore:  61%|██████▏   | 18.3M/29.8M [00:38<00:19, 584k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 41; Resources: 37.0 CPU, 2.3GB object s
  tore:  61%|██████▏   | 18.3M/29.8M [00:38<00:19, 584k row/s]
  
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 33.3MB object store; [22/108
  objects local]:  55%|█████▌    | 16.6M/29.9M [00:38<00:16, 809k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 33.3MB object store; [22/108
  objects local]:  56%|█████▌    | 16.6M/29.8M [00:38<00:16, 809k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 33.3MB object store; [22/108
  objects local]:  56%|█████▌    | 16.7M/29.8M [00:38<00:21, 609k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.3GB/39.9GB object store:  56%|█████▌    | 16.6M/29.7M [00:39<00:20, 638k row/s]M/29.8M [00:38<00:21, 609k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.3GB/39.9GB object store:  57%|█████▋    | 16.9M/29.7M [00:39<00:23, 544k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.3GB/39.9GB object store:  57%|█████▋    | 16.9M/29.7M [00:39<00:23, 544k row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 37; Resources: 37.0 CPU, 2.3GB object s
  tore:  61%|██████▏   | 18.3M/29.8M [00:39<00:19, 584k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 37; Resources: 37.0 CPU, 2.3GB object s
  tore:  62%|██████▏   | 18.3M/29.4M [00:39<00:19, 584k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 37; Resources: 37.0 CPU, 2.3GB object s
  tore:  63%|██████▎   | 18.7M/29.4M [00:39<00:20, 528k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 37; Resources: 37.0 CPU, 2.3GB object s
  tore:  63%|██████▎   | 18.7M/29.4M [00:39<00:20, 528k row/s]
  
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 33.3MB object store; [23/111
  objects local]:  56%|█████▌    | 16.7M/29.8M [00:39<00:21, 609k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 33.3MB object store; [23/111
  objects local]:  57%|█████▋    | 16.7M/29.4M [00:39<00:20, 609k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 33.3MB object store; [23/111
  objects local]:  58%|█████▊    | 17.1M/29.4M [00:39<00:22, 546k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.3GB/39.9GB object store:  58%|█████▊    | 16.9M/29.2M [00:40<00:22, 544k row/s]M/29.4M [00:39<00:22, 546k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.3GB/39.9GB object store:  60%|██████    | 17.5M/29.2M [00:40<00:20, 555k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.3GB/39.9GB object store:  60%|██████    | 17.5M/29.2M [00:40<00:20, 555k row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 32; Resources: 37.0 CPU, 2.2GB object s
  tore:  63%|██████▎   | 18.7M/29.4M [00:40<00:20, 528k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 32; Resources: 37.0 CPU, 2.2GB object s
  tore:  64%|██████▍   | 18.7M/29.1M [00:40<00:19, 528k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 32; Resources: 37.0 CPU, 2.2GB object s
  tore:  66%|██████▌   | 19.2M/29.1M [00:40<00:19, 516k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 32; Resources: 37.0 CPU, 2.2GB object s
  tore:  66%|██████▌   | 19.2M/29.1M [00:40<00:19, 516k row/s]
  
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 33.3MB object store; [26/117
  objects local]:  58%|█████▊    | 17.1M/29.4M [00:40<00:22, 546k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 33.3MB object store; [26/117
  objects local]:  59%|█████▊    | 17.1M/29.2M [00:40<00:22, 546k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 33.3MB object store; [26/117
  objects local]:  60%|██████    | 17.6M/29.2M [00:40<00:21, 529k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 36/37 CPU, 2.2GB/39.9GB object store:  60%|██████    | 17.5M/29.2M [00:41<00:20, 555k row/s]M/29.2M [00:40<00:21, 529k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 36/37 CPU, 2.2GB/39.9GB object store:  62%|██████▏   | 17.5M/28.3M [00:41<00:19, 555k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 36/37 CPU, 2.2GB/39.9GB object store:  66%|██████▋   | 18.8M/28.3M [00:41<00:12, 737k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 36/37 CPU, 2.2GB/39.9GB object store:  66%|██████▋   | 18.8M/28.3M [00:41<00:12, 737k row/s]
  
  
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 66.6MB object store; [31/131
  objects local]:  60%|██████    | 17.6M/29.2M [00:41<00:21, 529k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 66.6MB object store; [31/131
  objects local]:  62%|██████▏   | 17.6M/28.2M [00:41<00:20, 529k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 66.6MB object store; [31/131
  objects local]:  68%|██████▊   | 19.1M/28.2M [00:41<00:11, 779k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 66.6MB object store; [31/131
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 17; Resources: 37.0 CPU, 2.2GB object s
  tore:  66%|██████▌   | 19.2M/29.1M [00:41<00:19, 516k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 17; Resources: 37.0 CPU, 2.2GB object s
  tore:  68%|██████▊   | 19.2M/28.2M [00:41<00:17, 516k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 17; Resources: 37.0 CPU, 2.2GB object s
  tore:  73%|███████▎  | 20.6M/28.2M [00:41<00:10, 744k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 17; Resources: 37.0 CPU, 2.2GB object s
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.2GB/39.9GB object store:  66%|██████▋   | 18.8M/28.3M [00:42<00:12, 737k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.2GB/39.9GB object store:  67%|██████▋   | 18.8M/28.1M [00:42<00:12, 737k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.2GB/39.9GB object store:  69%|██████▉   | 19.4M/28.1M [00:42<00:12, 694k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.2GB/39.9GB object store:  69%|██████▉   | 19.4M/28.1M [00:42<00:12, 694k row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 13; Resources: 37.0 CPU, 2.2GB object s
  tore:  73%|███████▎  | 20.6M/28.2M [00:42<00:10, 744k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 13; Resources: 37.0 CPU, 2.2GB object s
  tore:  74%|███████▎  | 20.6M/28.0M [00:42<00:09, 744k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 13; Resources: 37.0 CPU, 2.2GB object s
  tore:  75%|███████▌  | 21.0M/28.0M [00:42<00:10, 641k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 13; Resources: 37.0 CPU, 2.2GB object s
  tore:  75%|███████▌  | 21.0M/28.0M [00:42<00:10, 641k row/s]
  
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 0.0B object store; [31/135 ob
  jects local]:  68%|██████▊   | 19.1M/28.2M [00:42<00:11, 779k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 0.0B object store; [31/135 ob
  jects local]:  68%|██████▊   | 19.1M/28.0M [00:42<00:11, 779k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 0.0B object store; [31/135 ob
  jects local]:  70%|██████▉   | 19.5M/28.0M [00:42<00:12, 666k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.2GB/39.9GB object store:  70%|██████▉   | 19.4M/27.7M [00:43<00:12, 694k row/s]28.0M [00:42<00:12, 666k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.2GB/39.9GB object store:  72%|███████▏  | 20.0M/27.7M [00:43<00:11, 662k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 37/37 CPU, 2.2GB/39.9GB object store:  72%|███████▏  | 20.0M/27.7M [00:43<00:11, 662k row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 7; Resources: 37.0 CPU, 2.2GB object st
  ore:  75%|███████▌  | 21.0M/28.0M [00:43<00:10, 641k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 7; Resources: 37.0 CPU, 2.2GB object st
  ore:  76%|███████▌  | 21.0M/27.7M [00:43<00:10, 641k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 7; Resources: 37.0 CPU, 2.2GB object st
  ore:  78%|███████▊  | 21.6M/27.7M [00:43<00:09, 624k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 7; Resources: 37.0 CPU, 2.2GB object st
  ore:  78%|███████▊  | 21.6M/27.7M [00:43<00:09, 624k row/s]
  
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 0.0B object store; [31/141 ob
  jects local]:  70%|██████▉   | 19.5M/28.0M [00:43<00:12, 666k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 0.0B object store; [31/141 ob
  jects local]:  70%|███████   | 19.5M/27.7M [00:43<00:12, 666k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 0.0B object store; [31/141 ob
  jects local]:  73%|███████▎  | 20.1M/27.7M [00:43<00:11, 643k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 35/37 CPU, 2.1GB/39.9GB object store:  72%|███████▏  | 20.0M/27.7M [00:44<00:11, 662k row/s]27.7M [00:43<00:11, 643k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 35/37 CPU, 2.1GB/39.9GB object store:  72%|███████▏  | 20.0M/27.6M [00:44<00:11, 662k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 35/37 CPU, 2.1GB/39.9GB object store:  74%|███████▎  | 20.3M/27.6M [00:44<00:13, 554k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 35/37 CPU, 2.1GB/39.9GB object store:  74%|███████▎  | 20.3M/27.6M [00:44<00:13, 554k row/s]
  
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 3; Resources: 37.0 CPU, 2.1GB object st
  ore:  78%|███████▊  | 21.6M/27.7M [00:44<00:09, 624k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 3; Resources: 37.0 CPU, 2.1GB object st
  ore:  79%|███████▊  | 21.6M/27.5M [00:44<00:09, 624k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 3; Resources: 37.0 CPU, 2.1GB object st
  ore:  80%|████████  | 22.0M/27.5M [00:44<00:09, 555k row/s]
  (pid=2719) - ReadParquet: Tasks: 37 [backpressured:tasks]; Actors: 0; Queued blocks: 3; Resources: 37.0 CPU, 2.1GB object st
  ore:  80%|████████  | 22.0M/27.5M [00:44<00:09, 555k row/s]
  
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 0.0B object store; [32/145 ob
  jects local]:  73%|███████▎  | 20.1M/27.7M [00:44<00:11, 643k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 0.0B object store; [32/145 ob
  jects local]:  73%|███████▎  | 20.1M/27.5M [00:44<00:11, 643k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 0.0B object store; [32/145 ob
  jects local]:  75%|███████▍  | 20.5M/27.5M [00:44<00:12, 568k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 35/37 CPU, 2.0GB/39.9GB object store:  74%|███████▎  | 20.3M/27.6M [00:45<00:13, 554k row/s]27.5M [00:44<00:12, 568k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 35/37 CPU, 2.0GB/39.9GB object store:  74%|███████▍  | 20.3M/27.3M [00:45<00:12, 554k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 35/37 CPU, 2.0GB/39.9GB object store:  77%|███████▋  | 21.0M/27.3M [00:45<00:10, 591k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 35/37 CPU, 2.0GB/39.9GB object store:  77%|███████▋  | 21.0M/27.3M [00:45<00:10, 591k row/s]
  
  (pid=2719) - ReadParquet: Tasks: 35; Actors: 0; Queued blocks: 0; Resources: 35.0 CPU, 2.0GB object store:  80%|████████  |
  22.0M/27.5M [00:45<00:09, 555k row/s]
  (pid=2719) - ReadParquet: Tasks: 35; Actors: 0; Queued blocks: 0; Resources: 35.0 CPU, 2.0GB object store:  81%|████████  |
  22.0M/27.3M [00:45<00:09, 555k row/s]
  (pid=2719) - ReadParquet: Tasks: 35; Actors: 0; Queued blocks: 0; Resources: 35.0 CPU, 2.0GB object store:  83%|████████▎ |
  22.5M/27.3M [00:45<00:08, 535k row/s]
  (pid=2719) - ReadParquet: Tasks: 35; Actors: 0; Queued blocks: 0; Resources: 35.0 CPU, 2.0GB object store:  83%|████████▎ |
  22.5M/27.3M [00:45<00:08, 535k row/s]
  
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 0.0B object store; [33/150 ob
  jects local]:  75%|███████▍  | 20.5M/27.5M [00:45<00:12, 568k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 0.0B object store; [33/150 ob
  jects local]:  75%|███████▌  | 20.5M/27.3M [00:45<00:11, 568k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 0.0B object store; [33/150 ob
  jects local]:  77%|███████▋  | 21.0M/27.3M [00:45<00:11, 544k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 25/37 CPU, 1.7GB/39.9GB object store:  77%|███████▋  | 21.0M/27.3M [00:46<00:10, 591k row/s]27.3M [00:45<00:11, 544k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 25/37 CPU, 1.7GB/39.9GB object store:  78%|███████▊  | 21.0M/26.9M [00:46<00:10, 591k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 25/37 CPU, 1.7GB/39.9GB object store:  80%|████████  | 21.6M/26.9M [00:46<00:09, 590k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 25/37 CPU, 1.7GB/39.9GB object store:  80%|████████  | 21.6M/26.9M [00:46<00:09, 590k row/s]
  
  (pid=2719) - ReadParquet: Tasks: 23; Actors: 0; Queued blocks: 0; Resources: 23.0 CPU, 1.5GB object store:  83%|████████▎ |
  22.5M/27.3M [00:46<00:08, 535k row/s]
  (pid=2719) - ReadParquet: Tasks: 23; Actors: 0; Queued blocks: 0; Resources: 23.0 CPU, 1.5GB object store:  84%|████████▍ |
  22.5M/26.7M [00:46<00:07, 535k row/s]
  (pid=2719) - ReadParquet: Tasks: 23; Actors: 0; Queued blocks: 0; Resources: 23.0 CPU, 1.5GB object store:  89%|████████▉ |
  23.8M/26.7M [00:46<00:03, 753k row/s]
  (pid=2719) - ReadParquet: Tasks: 23; Actors: 0; Queued blocks: 0; Resources: 23.0 CPU, 1.5GB object store:  89%|████████▉ |
  23.8M/26.7M [00:46<00:03, 753k row/s]
  
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 133.3MB object store; [39/162
   objects local]:  77%|███████▋  | 21.0M/27.3M [00:46<00:11, 544k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 133.3MB object store; [39/162
   objects local]:  79%|███████▊  | 21.0M/26.7M [00:46<00:10, 544k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 133.3MB object store; [39/162
   objects local]:  83%|████████▎ | 22.1M/26.7M [00:46<00:06, 701k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 16/37 CPU, 1.2GB/39.9GB object store:  80%|████████  | 21.6M/26.9M [00:47<00:09, 590k row/s]1M/26.7M [00:46<00:06, 701k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 16/37 CPU, 1.2GB/39.9GB object store:  81%|████████▏ | 21.6M/26.6M [00:47<00:08, 590k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 16/37 CPU, 1.2GB/39.9GB object store:  86%|████████▌ | 22.8M/26.6M [00:47<00:04, 762k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 16/37 CPU, 1.2GB/39.9GB object store:  86%|████████▌ | 22.8M/26.6M [00:47<00:04, 762k row/s]
  
  (pid=2719) - ReadParquet: Tasks: 15; Actors: 0; Queued blocks: 0; Resources: 15.0 CPU, 1.1GB object store:  89%|████████▉ |
  23.8M/26.7M [00:47<00:03, 753k row/s]
  (pid=2719) - ReadParquet: Tasks: 15; Actors: 0; Queued blocks: 0; Resources: 15.0 CPU, 1.1GB object store:  90%|████████▉ |
  23.8M/26.5M [00:47<00:03, 753k row/s]
  (pid=2719) - ReadParquet: Tasks: 15; Actors: 0; Queued blocks: 0; Resources: 15.0 CPU, 1.1GB object store:  93%|█████████▎|
  24.6M/26.5M [00:47<00:02, 761k row/s]
  (pid=2719) - ReadParquet: Tasks: 15; Actors: 0; Queued blocks: 0; Resources: 15.0 CPU, 1.1GB object store:  93%|█████████▎|
  24.6M/26.5M [00:47<00:02, 761k row/s]
  
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 66.6MB object store; [40/170
  objects local]:  83%|████████▎ | 22.1M/26.7M [00:47<00:06, 701k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 66.6MB object store; [40/170
  objects local]:  84%|████████▎ | 22.1M/26.5M [00:47<00:06, 701k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 66.6MB object store; [40/170
  objects local]:  87%|████████▋ | 23.0M/26.5M [00:47<00:04, 753k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 12/37 CPU, 1.0GB/39.9GB object store:  86%|████████▌ | 22.8M/26.6M [00:48<00:04, 762k row/s]M/26.5M [00:47<00:04, 753k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 12/37 CPU, 1.0GB/39.9GB object store:  86%|████████▋ | 22.8M/26.4M [00:48<00:04, 762k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 12/37 CPU, 1.0GB/39.9GB object store:  88%|████████▊ | 23.3M/26.4M [00:48<00:04, 680k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 12/37 CPU, 1.0GB/39.9GB object store:  88%|████████▊ | 23.3M/26.4M [00:48<00:04, 680k row/s]
  
  
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 33.3MB object store; [40/174
  objects local]:  87%|████████▋ | 23.0M/26.5M [00:48<00:04, 753k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 33.3MB object store; [40/174
  objects local]:  87%|████████▋ | 23.0M/26.3M [00:48<00:04, 753k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 33.3MB object store; [40/174
  objects local]:  89%|████████▉ | 23.4M/26.3M [00:48<00:04, 645k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 33.3MB object store; [40/174
  (pid=2719) - ReadParquet: Tasks: 11; Actors: 0; Queued blocks: 0; Resources: 11.0 CPU, 982.6MB object store:  93%|█████████▎
  | 24.6M/26.5M [00:48<00:02, 761k row/s]
  (pid=2719) - ReadParquet: Tasks: 11; Actors: 0; Queued blocks: 0; Resources: 11.0 CPU, 982.6MB object store:  93%|█████████▎
  | 24.6M/26.3M [00:48<00:02, 761k row/s]
  (pid=2719) - ReadParquet: Tasks: 11; Actors: 0; Queued blocks: 0; Resources: 11.0 CPU, 982.6MB object store:  95%|█████████▍
  | 24.9M/26.3M [00:48<00:02, 621k row/s]
  (pid=2719) - ReadParquet: Tasks: 11; Actors: 0; Queued blocks: 0; Resources: 11.0 CPU, 982.6MB object store:  95%|█████████▍
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 5/37 CPU, 784.3MB/39.9GB object store:  88%|████████▊ | 23.3M/26.4M [00:49<00:04, 680k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 5/37 CPU, 784.3MB/39.9GB object store:  89%|████████▉ | 23.3M/26.2M [00:49<00:04, 680k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 5/37 CPU, 784.3MB/39.9GB object store:  91%|█████████ | 23.8M/26.2M [00:49<00:03, 622k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 5/37 CPU, 784.3MB/39.9GB object store:  91%|█████████ | 23.8M/26.2M [00:49<00:03, 622k row/s]
  
  (pid=2719) - ReadParquet: Tasks: 5; Actors: 0; Queued blocks: 0; Resources: 5.0 CPU, 717.6MB object store:  95%|█████████▍|
  24.9M/26.3M [00:49<00:02, 621k row/s]
  (pid=2719) - ReadParquet: Tasks: 5; Actors: 0; Queued blocks: 0; Resources: 5.0 CPU, 717.6MB object store:  95%|█████████▌|
  24.9M/26.2M [00:49<00:02, 621k row/s]
  (pid=2719) - ReadParquet: Tasks: 5; Actors: 0; Queued blocks: 0; Resources: 5.0 CPU, 717.6MB object store:  98%|█████████▊|
  25.5M/26.2M [00:49<00:01, 610k row/s]
  (pid=2719) - ReadParquet: Tasks: 5; Actors: 0; Queued blocks: 0; Resources: 5.0 CPU, 717.6MB object store:  98%|█████████▊|
  25.5M/26.2M [00:49<00:01, 610k row/s]
  
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 0.0B object store; [40/180 ob
  jects local]:  89%|████████▉ | 23.4M/26.3M [00:49<00:04, 645k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 0.0B object store; [40/180 ob
  jects local]:  89%|████████▉ | 23.4M/26.2M [00:49<00:04, 645k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 0.0B object store; [40/180 ob
  jects local]:  92%|█████████▏| 24.0M/26.2M [00:49<00:03, 627k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 3/37 CPU, 630.2MB/39.9GB object store:  91%|█████████ | 23.8M/26.2M [00:50<00:03, 622k row/s]6.2M [00:49<00:03, 627k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 3/37 CPU, 630.2MB/39.9GB object store:  91%|█████████ | 23.8M/26.1M [00:50<00:03, 622k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 3/37 CPU, 630.2MB/39.9GB object store:  93%|█████████▎| 24.2M/26.1M [00:50<00:03, 553k row/s]
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 3/37 CPU, 630.2MB/39.9GB object store:  93%|█████████▎| 24.2M/26.1M [00:50<00:03, 553k row/s]
  
  
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 33.3MB object store; [41/183
  objects local]:  92%|█████████▏| 24.0M/26.2M [00:50<00:03, 627k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 33.3MB object store; [41/183
  objects local]:  92%|█████████▏| 24.0M/26.1M [00:50<00:03, 627k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 33.3MB object store; [41/183
  objects local]:  93%|█████████▎| 24.3M/26.1M [00:50<00:03, 527k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 33.3MB object store; [41/183
  (pid=2719) - ReadParquet: Tasks: 2; Actors: 0; Queued blocks: 0; Resources: 2.0 CPU, 586.6MB object store:  98%|█████████▊|
  25.5M/26.2M [00:50<00:01, 610k row/s]
  (pid=2719) - ReadParquet: Tasks: 2; Actors: 0; Queued blocks: 0; Resources: 2.0 CPU, 586.6MB object store:  98%|█████████▊|
  25.5M/26.1M [00:50<00:00, 610k row/s]
  (pid=2719) - ReadParquet: Tasks: 2; Actors: 0; Queued blocks: 0; Resources: 2.0 CPU, 586.6MB object store:  99%|█████████▉|
  25.8M/26.1M [00:50<00:00, 515k row/s]
  (pid=2719) - ReadParquet: Tasks: 2; Actors: 0; Queued blocks: 0; Resources: 2.0 CPU, 586.6MB object store:  99%|█████████▉|
                                                                                                                              
  
                                                                                                                              
  
  
  (RayTrainWorker pid=2523) [07:20:51] Task [xgboost.ray-rank=00000002]:4024a9c63fbd12e440923df002000000 got rank 2 [repeated 7x across cluster] (Ray deduplicates logs by default. Set RAY_DEDUP_LOGS=0 to disable log deduplication, or see https://docs.ray.io/en/master/ray-observability/user-guides/configure-logging.html#log-deduplication for more options.)
  
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 3/37 CPU, 630.2MB/39.9GB object store:  93%|█████████▎| 24.2M/26.1M [00:51<00:03, 553k row/s]
  
  (pid=2719) - ReadParquet: Tasks: 2; Actors: 0; Queued blocks: 0; Resources: 2.0 CPU, 586.6MB object store:  99%|█████████▉|
  25.8M/26.1M [00:50<00:00, 515k row/s]
  
                                                                                                                                                                    /26.1M [00:50<00:03, 527k row/s]
  
                                                                                                                              
  
  
  (RayTrainWorker pid=2522) Registered dataset logger for dataset dataset_4_0
  
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 3/37 CPU, 630.2MB/39.9GB object store:  93%|█████████▎| 24.2M/26.1M [00:51<00:03, 553k row/s]
  
  (pid=2719) - ReadParquet: Tasks: 2; Actors: 0; Queued blocks: 0; Resources: 2.0 CPU, 586.6MB object store:  99%|█████████▉|
  25.8M/26.1M [00:50<00:00, 515k row/s]
  
                                                                                                                                                                    /26.1M [00:50<00:03, 527k row/s]
  
                                                                                                                              
  
  
  (RayTrainWorker pid=2522) ⚠️  Ray's object store is configured to use only 29.6% of available memory (79.9GiB out of 270.0GiB total). For optimal Ray Data performance, we recommend setting the object store to at least 50% of available memory. You can do this by setting the 'object_store_memory' parameter when calling ray.init() or by setting the RAY_DEFAULT_OBJECT_STORE_MEMORY_PROPORTION environment variable.
  
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 3/37 CPU, 630.2MB/39.9GB object store:  93%|█████████▎| 24.2M/26.1M [00:51<00:03, 553k row/s]
  
  (pid=2719) - ReadParquet: Tasks: 2; Actors: 0; Queued blocks: 0; Resources: 2.0 CPU, 586.6MB object store:  99%|█████████▉|
  25.8M/26.1M [00:50<00:00, 515k row/s]
  
                                                                                                                                                                    /26.1M [00:50<00:03, 527k row/s]
  
                                                                                                                              
  
  
  (RayTrainWorker pid=2521) Registered dataset logger for dataset dataset_5_0
  
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 3/37 CPU, 630.2MB/39.9GB object store:  93%|█████████▎| 24.2M/26.1M [00:51<00:03, 553k row/s]
  
  (pid=2719) - ReadParquet: Tasks: 2; Actors: 0; Queued blocks: 0; Resources: 2.0 CPU, 586.6MB object store:  99%|█████████▉|
  25.8M/26.1M [00:50<00:00, 515k row/s]
  
                                                                                                                                                                    /26.1M [00:50<00:03, 527k row/s]
  
                                                                                                                              
  
  
  (RayTrainWorker pid=2521) ⚠️  Ray's object store is configured to use only 29.6% of available memory (79.9GiB out of 270.0GiB total). For optimal Ray Data performance, we recommend setting the object store to at least 50% of available memory. You can do this by setting the 'object_store_memory' parameter when calling ray.init() or by setting the RAY_DEFAULT_OBJECT_STORE_MEMORY_PROPORTION environment variable.
  
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 3/37 CPU, 630.2MB/39.9GB object store:  93%|█████████▎| 24.2M/26.1M [00:51<00:03, 553k row/s]
  
  (pid=2719) - ReadParquet: Tasks: 2; Actors: 0; Queued blocks: 0; Resources: 2.0 CPU, 586.6MB object store:  99%|█████████▉|
  25.8M/26.1M [00:50<00:00, 515k row/s]
  
                                                                                                                                                                    /26.1M [00:50<00:03, 527k row/s]
  
                                                                                                                              
  
  
  (SplitCoordinator pid=2719) ✔️  Dataset train_1_0 execution finished in 51.08 seconds
  
  (pid=2719) Running Dataset: train_1_0. Active & requested resources: 3/37 CPU, 630.2MB/39.9GB object store:  93%|█████████▎| 24.2M/26.1M [00:51<00:03, 553k row/s]
  
  (pid=2719) - ReadParquet: Tasks: 2; Actors: 0; Queued blocks: 0; Resources: 2.0 CPU, 586.6MB object store:  99%|█████████▉|
  25.8M/26.1M [00:50<00:00, 515k row/s]
  
  (pid=2719) ✔️  Dataset train_1_0 execution finished in 51.08 seconds:  93%|█████████▎| 24.2M/26.1M [00:51<00:03, 553k row/s]                                      /26.1M [00:50<00:03, 527k row/s]
  (pid=2719) ✔️  Dataset train_1_0 execution finished in 51.08 seconds:  93%|█████████▎| 24.2M/26.0M [00:51<00:03, 553k row/s]
  (pid=2719) ✔️  Dataset train_1_0 execution finished in 51.08 seconds: 100%|██████████| 26.0M/26.0M [00:51<00:00, 1.11M row/s]
  (pid=2719) ✔️  Dataset train_1_0 execution finished in 51.08 seconds: 100%|██████████| 26.0M/26.0M [00:51<00:00, 1.11M row/s]
                                                                                                                              
  
                                                                                                                              
  
  
  
  (pid=2719) ✔️  Dataset train_1_0 execution finished in 51.08 seconds: 100%|██████████| 26.0M/26.0M [00:51<00:00, 509k row/s]
  
  
  (pid=2719) - ReadParquet: Tasks: 2; Actors: 0; Queued blocks: 0; Resources: 2.0 CPU, 586.6MB object store:  99%|█████████▉|
  25.8M/26.1M [00:50<00:00, 515k row/s]
  
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 33.3MB object store; [41/183
  (pid=2719) - ReadParquet: Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 0.0B object store:  99%|█████████▉| 25.
  8M/26.1M [00:50<00:00, 515k row/s]
  (pid=2719) - ReadParquet: Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 0.0B object store:  99%|█████████▉| 25.
  8M/26.0M [00:50<00:00, 515k row/s]
  (pid=2719) - ReadParquet: Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 0.0B object store: 100%|██████████| 26.
  0M/26.0M [00:50<00:00, 550k row/s]
  (pid=2719) - ReadParquet: Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 0.0B object store: 100%|██████████| 26.
  0M/26.0M [00:50<00:00, 550k row/s]
                                                                                                                              
  
  
  
  (pid=2719) - ReadParquet: Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 0.0B object store: 100%|██████████| 26.0M/26.0M [00:50<00:00, 511k row/s]
  
  
  
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 15; Resources: 0.0 CPU, 33.3MB object store; [41/183
  objects local]:  93%|█████████▎| 24.3M/26.1M [00:50<00:03, 527k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 566.4MB object store; [42/185
  objects local]:  93%|█████████▎| 24.3M/26.1M [00:50<00:03, 527k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 566.4MB object store; [42/185
  objects local]:  93%|█████████▎| 24.3M/26.0M [00:50<00:03, 527k row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 566.4MB object store; [42/185
  objects local]: 100%|██████████| 26.0M/26.0M [00:50<00:00, 1.13M row/s]
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 566.4MB object store; [42/185
  objects local]: 100%|██████████| 26.0M/26.0M [00:50<00:00, 1.13M row/s]
  
  (pid=2719) - split(8, equal=True): Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 566.4MB object store; [42/185 objects local]: 100%|██████████| 26.0M/26.0M [00:50<00:00, 511k row/s]
  
  Training finished iteration 1 at 2025-10-14 07:21:59. Total running time: 1min 12s
  ╭───────────────────────────────╮
  │ Training result               │
  ├───────────────────────────────┤
  │ checkpoint_dir_name           │
  │ time_this_iter_s      69.7634 │
  │ time_total_s          69.7634 │
  │ training_iteration          1 │
  │ train-error           0.23532 │
  │ train-logloss         0.61182 │
  ╰───────────────────────────────╯
  (XGBoostTrainer pid=2433) [07:21:59] [0]        train-logloss:0.61182   train-error:0.23532
  (RayTrainWorker pid=417, ip=10.244.1.106) Registered dataset logger for dataset dataset_8_0 [repeated 6x across cluster]
  (RayTrainWorker pid=417, ip=10.244.1.106) ⚠️  Ray's object store is configured to use only 29.6% of available memory (79.9GiB out of 270.0GiB total). For optimal Ray Data performance, we recommend setting the object store to at least 50% of available memory. You can do this by setting the 'object_store_memory' parameter when calling ray.init() or by setting the RAY_DEFAULT_OBJECT_STORE_MEMORY_PROPORTION environment variable. [repeated 6x across cluster]
  
  Training finished iteration 2 at 2025-10-14 07:21:59. Total running time: 1min 12s
  ╭───────────────────────────────╮
  │ Training result               │
  ├───────────────────────────────┤
  │ checkpoint_dir_name           │
  │ time_this_iter_s      0.78981 │
  │ time_total_s          70.5532 │
  │ training_iteration          2 │
  │ train-error           0.20581 │
  │ train-logloss         0.55808 │
  ╰───────────────────────────────╯
  (XGBoostTrainer pid=2433) [07:21:59] [1]        train-logloss:0.55808   train-error:0.20581
  
  Training finished iteration 3 at 2025-10-14 07:22:00. Total running time: 1min 13s
  ╭───────────────────────────────╮
  │ Training result               │
  ├───────────────────────────────┤
  │ checkpoint_dir_name           │
  │ time_this_iter_s      0.74851 │
  │ time_total_s          71.3017 │
  │ training_iteration          3 │
  │ train-error           0.19293 │
  │ train-logloss         0.52137 │
  ╰───────────────────────────────╯
  (XGBoostTrainer pid=2433) [07:22:00] [2]        train-logloss:0.52137   train-error:0.19293
  
  Training finished iteration 4 at 2025-10-14 07:22:01. Total running time: 1min 14s
  ╭───────────────────────────────╮
  │ Training result               │
  ├───────────────────────────────┤
  │ checkpoint_dir_name           │
  │ time_this_iter_s      0.73066 │
  │ time_total_s          72.0324 │
  │ training_iteration          4 │
  │ train-error           0.17964 │
  │ train-logloss         0.49252 │
  ╰───────────────────────────────╯
  (XGBoostTrainer pid=2433) [07:22:01] [3]        train-logloss:0.49252   train-error:0.17964
  
  Training finished iteration 5 at 2025-10-14 07:22:02. Total running time: 1min 15s
  ╭───────────────────────────────╮
  │ Training result               │
  ├───────────────────────────────┤
  │ checkpoint_dir_name           │
  │ time_this_iter_s      0.76333 │
  │ time_total_s          72.7957 │
  │ training_iteration          5 │
  │ train-error           0.17183 │
  │ train-logloss          0.4707 │
  ╰───────────────────────────────╯
  (XGBoostTrainer pid=2433) [07:22:02] [4]        train-logloss:0.47070   train-error:0.17183
  
  Training finished iteration 6 at 2025-10-14 07:22:02. Total running time: 1min 15s
  ╭───────────────────────────────╮
  │ Training result               │
  ├───────────────────────────────┤
  │ checkpoint_dir_name           │
  │ time_this_iter_s      0.73944 │
  │ time_total_s          73.5352 │
  │ training_iteration          6 │
  │ train-error           0.16559 │
  │ train-logloss         0.45633 │
  ╰───────────────────────────────╯
  (XGBoostTrainer pid=2433) [07:22:02] [5]        train-logloss:0.45633   train-error:0.16559
  
  Training finished iteration 7 at 2025-10-14 07:22:03. Total running time: 1min 16s
  ╭───────────────────────────────╮
  │ Training result               │
  ├───────────────────────────────┤
  │ checkpoint_dir_name           │
  │ time_this_iter_s      0.74472 │
  │ time_total_s          74.2799 │
  │ training_iteration          7 │
  │ train-error           0.15971 │
  │ train-logloss         0.44288 │
  ╰───────────────────────────────╯
  (XGBoostTrainer pid=2433) [07:22:03] [6]        train-logloss:0.44288   train-error:0.15971
  
  Training finished iteration 8 at 2025-10-14 07:22:04. Total running time: 1min 17s
  ╭───────────────────────────────╮
  │ Training result               │
  ├───────────────────────────────┤
  │ checkpoint_dir_name           │
  │ time_this_iter_s      0.69141 │
  │ time_total_s          74.9713 │
  │ training_iteration          8 │
  │ train-error           0.15582 │
  │ train-logloss         0.43268 │
  ╰───────────────────────────────╯
  (XGBoostTrainer pid=2433) [07:22:04] [7]        train-logloss:0.43268   train-error:0.15582
  
  Training finished iteration 9 at 2025-10-14 07:22:05. Total running time: 1min 18s
  ╭───────────────────────────────╮
  │ Training result               │
  ├───────────────────────────────┤
  │ checkpoint_dir_name           │
  │ time_this_iter_s       0.6992 │
  │ time_total_s          75.6705 │
  │ training_iteration          9 │
  │ train-error           0.15076 │
  │ train-logloss         0.42349 │
  ╰───────────────────────────────╯
  (XGBoostTrainer pid=2433) [07:22:05] [8]        train-logloss:0.42349   train-error:0.15076
  
  Training finished iteration 10 at 2025-10-14 07:22:05. Total running time: 1min 18s
  ╭───────────────────────────────╮
  │ Training result               │
  ├───────────────────────────────┤
  │ checkpoint_dir_name           │
  │ time_this_iter_s      0.73254 │
  │ time_total_s           76.403 │
  │ training_iteration         10 │
  │ train-error           0.14671 │
  │ train-logloss         0.41614 │
  ╰───────────────────────────────╯
  (XGBoostTrainer pid=2433) [07:22:05] [9]        train-logloss:0.41614   train-error:0.14671
  
  Training finished iteration 11 at 2025-10-14 07:22:12. Total running time: 1min 25s
  ╭─────────────────────────────────────────╮
  │ Training result                         │
  ├─────────────────────────────────────────┤
  │ checkpoint_dir_name   checkpoint_000000 │
  │ time_this_iter_s                6.41393 │
  │ time_total_s                   82.81696 │
  │ training_iteration                   11 │
  │ train-error                     0.14671 │
  │ train-logloss                   0.41614 │
  ╰─────────────────────────────────────────╯
  Training saved a checkpoint for iteration 11 at: (abfs)traineddata/cluster_storage/xgboost_benchmark/XGBoostTrainer_f8a55_00000_0_2025-10-14_07-20-46/checkpoint_000000
  (RayTrainWorker pid=2521) Checkpoint successfully created at: Checkpoint(filesystem=abfs, path=traineddata/cluster_storage/xgboost_benchmark/XGBoostTrainer_f8a55_00000_0_2025-10-14_07-20-46/checkpoint_000000)
  
  Training completed after 11 iterations at 2025-10-14 07:22:13. Total running time: 1min 26s
  2025-10-14 07:22:17,163 INFO tune.py:1009 -- Wrote the latest version of all result files and experiment state to 'traineddata/cluster_storage/xgboost_benchmark' in 3.7461s.
  
  Running xgboost prediction benchmark...
  
  Parquet dataset sampling 0:   0%|          | 0.00/3.00 [00:00<?, ? file/s]
  Parquet dataset sampling 0:   0%|          | 0.00/3.00 [00:01<?, ? file/s]
  Parquet dataset sampling 0:   0%|          | 0.00/3.00 [00:02<?, ? file/s]
  Parquet dataset sampling 0:   0%|          | 0.00/3.00 [00:03<?, ? file/s]
  Parquet dataset sampling 0:   0%|          | 0.00/3.00 [00:04<?, ? file/s]
  Parquet dataset sampling 0:   0%|          | 0.00/3.00 [00:05<?, ? file/s]
  Parquet dataset sampling 0:   0%|          | 0.00/3.00 [00:06<?, ? file/s]
  Parquet dataset sampling 0:   0%|          | 0.00/3.00 [00:07<?, ? file/s]
  Parquet dataset sampling 0:   0%|          | 0.00/3.00 [00:08<?, ? file/s]
  Parquet dataset sampling 0:   0%|          | 0.00/3.00 [00:09<?, ? file/s]
  Parquet dataset sampling 0:   0%|          | 0.00/3.00 [00:10<?, ? file/s]
  Parquet dataset sampling 0:   0%|          | 0.00/3.00 [00:11<?, ? file/s]
  Parquet dataset sampling 0:   0%|          | 0.00/3.00 [00:12<?, ? file/s]
  Parquet dataset sampling 0:   0%|          | 0.00/3.00 [00:13<?, ? file/s]
  Parquet dataset sampling 0: 100%|██████████| 3.00/3.00 [00:13<00:00, 4.51s/ file]
  Parquet dataset sampling 0: 100%|██████████| 3.00/3.00 [00:13<00:00, 4.51s/ file]
  
  
  Parquet dataset sampling 0: 100%|██████████| 3.00/3.00 [00:13<00:00, 4.51s/ file]
  2025-10-14 07:22:34,705 INFO parquet_datasource.py:699 -- Estimated parquet encoding ratio is 0.850.
  2025-10-14 07:22:34,706 INFO parquet_datasource.py:759 -- Estimated parquet reader batch size at 383480 rows
  2025-10-14 07:22:36,441 INFO logging.py:293 -- Registered dataset logger for dataset dataset_15_0
  2025-10-14 07:22:36,461 INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_15_0. Full logs are in /tmp/ray/session_2025-10-14_07-18-54_398953_1/logs/ray-data
  2025-10-14 07:22:36,461 INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_15_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ReadParquet] -> ActorPoolMapOperator[MapBatches(drop_columns)->MapBatches(XGBoostPredictor)] -> TaskPoolMapOperator[Write]
  
  Running 0: 0.00 row [00:00, ? row/s][2025-10-14 07:22:36,892 E 1925 1925] core_worker.cc:2146: Actor with class name: 'MapWorker(MapBatches(drop_columns)->MapBatches(XGBoostPredictor))' and ID: 'dc601370a1bc5793965e00ab02000000' has constructor arguments in the object store and max_restarts > 0. If the arguments in the object store go out of scope or are lost, the actor restart will fail. See https://github.com/ray-project/ray/issues/53727 for more details.
  
  
  - ReadParquet 1: 0.00 row [00:00, ? row/s]
                                      2025-10-14 07:22:37,641     WARNING resource_manager.py:134 -- ⚠️  Ray's object store is configured to use only 29.6% of available memory (79.9GiB out of 270.0GiB total). For optimal Ray Data performance, we recommend setting the object store to at least 50% of available memory. You can do this by setting the 'object_store_memory' parameter when calling ray.init() or by setting the RAY_DEFAULT_OBJECT_STORE_MEMORY_PROPORTION environment variable.
  
  Running Dataset: dataset_15_0. Active & requested resources: 0/70 CPU, 0.0B/39.9GB object store (pending: 35 CPU): : 0.00 row [00:01, ? row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 0/70 CPU, 0.0B/39.9GB object store (pending: 35 CPU): : 0.00 row [00:01, ? row/s]
  
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 179; Resources: 21.0 CPU, 5.2GB object store: : 0.
  00 row [00:01, ? row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 179; Resources: 21.0 CPU, 5.2GB object store: : 0.
  00 row [00:01, ? row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35 (running=0, restarting=0, pending=35); Queued
   blocks: 0; Resources: 0.0 CPU, 0.0B object store; [all objects local]: : 0.00 row [00:01, ? row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35 (running=0, restarting=0, pending=35); Queued
   blocks: 0; Resources: 0.0 CPU, 0.0B object store; [all objects local]: : 0.00 row [00:01, ? row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 21/70 CPU, 5.2GB/39.9GB object store (pending: 35 CPU): : 0.00 row [00:02, ? row/s]ctors: 0; Queued blocks: 0; Resources: 0.0 CPU, 0.0B object store: : 0.00 row [00:01, ? row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 21/70 CPU, 5.2GB/39.9GB object store (pending: 35 CPU): : 0.00 row [00:02, ? row/s]
  
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 179; Resources: 21.0 CPU, 5.2GB object store: : 0.
  00 row [00:02, ? row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35 (running=0, restarting=0, pending=35); Queued
  Running Dataset: dataset_15_0. Active & requested resources: 21/70 CPU, 5.2GB/39.9GB object store (pending: 35 CPU): : 0.00 row [00:03, ? row/s]
  - Write: Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 0.0B object store: : 0.00 row [00:02, ? row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 179; Resources: 21.0 CPU, 5.2GB object store: : 0.
  00 row [00:03, ? row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35 (running=0, restarting=0, pending=35); Queued
  Running Dataset: dataset_15_0. Active & requested resources: 21/70 CPU, 5.2GB/39.9GB object store (pending: 35 CPU): : 0.00 row [00:04, ? row/s]
  - Write: Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 0.0B object store: : 0.00 row [00:03, ? row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 179; Resources: 21.0 CPU, 5.2GB object store: : 0.
  00 row [00:04, ? row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35 (running=0, restarting=0, pending=35); Queued
  Running Dataset: dataset_15_0. Active & requested resources: 21/70 CPU, 5.2GB/39.9GB object store (pending: 35 CPU): : 0.00 row [00:05, ? row/s]
  - Write: Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 0.0B object store: : 0.00 row [00:04, ? row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 179; Resources: 21.0 CPU, 5.2GB object store: : 0.
  00 row [00:05, ? row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35 (running=7, restarting=0, pending=28); Queued
   blocks: 0; Resources: 7.0 CPU, 0.0B object store; [all objects local]: : 0.00 row [00:05, ? row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35 (running=7, restarting=0, pending=28); Queued
  Running Dataset: dataset_15_0. Active & requested resources: 34/70 CPU, 5.2GB/39.9GB object store (pending: 22 CPU): : 0.00 row [00:06, ? row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 34/70 CPU, 5.2GB/39.9GB object store (pending: 22 CPU): : 0.00 row [00:06, ? row/s]
  
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 179; Resources: 21.0 CPU, 5.2GB object store: : 0.
  00 row [00:06, ? row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35 (running=28, restarting=0, pending=7); Queued
   blocks: 0; Resources: 28.0 CPU, 0.0B object store; [all objects local]: : 0.00 row [00:06, ? row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35 (running=28, restarting=0, pending=7); Queued
  Running Dataset: dataset_15_0. Active & requested resources: 49/70 CPU, 5.2GB/39.9GB object store (pending: 7 CPU): : 0.00 row [00:07, ? row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 49/70 CPU, 5.2GB/39.9GB object store (pending: 7 CPU): : 0.00 row [00:07, ? row/s]
  
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 179; Resources: 21.0 CPU, 5.2GB object store: : 0.
  00 row [00:07, ? row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 0.0B
  object store; [all objects local]: : 0.00 row [00:07, ? row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 0.0B
  Running Dataset: dataset_15_0. Active & requested resources: 56/70 CPU, 5.2GB/39.9GB object store: : 0.00 row [00:08, ? row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 56/70 CPU, 5.2GB/39.9GB object store: : 0.00 row [00:08, ? row/s]
  
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 179; Resources: 21.0 CPU, 5.2GB object store: : 0.
  00 row [00:08, ? row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 0.0B
  Running Dataset: dataset_15_0. Active & requested resources: 56/70 CPU, 5.2GB/39.9GB object store: : 0.00 row [00:09, ? row/s]
  - Write: Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 0.0B object store: : 0.00 row [00:08, ? row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 179; Resources: 21.0 CPU, 5.2GB object store: : 0.
  00 row [00:09, ? row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 0.0B
  Running Dataset: dataset_15_0. Active & requested resources: 56/70 CPU, 5.2GB/39.9GB object store: : 0.00 row [00:10, ? row/s]
  - Write: Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 0.0B object store: : 0.00 row [00:09, ? row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 179; Resources: 21.0 CPU, 5.2GB object store: : 0.
  00 row [00:10, ? row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 0.0B
  Running Dataset: dataset_15_0. Active & requested resources: 56/70 CPU, 5.2GB/39.9GB object store: : 0.00 row [00:11, ? row/s]
  - Write: Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 0.0B object store: : 0.00 row [00:10, ? row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 178; Resources: 21.0 CPU, 1.4GB object store: : 0.
  00 row [00:11, ? row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 178; Resources: 21.0 CPU, 1.4GB object store:   0%
  |          | 0.00/40.0M [00:11<?, ? row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 178; Resources: 21.0 CPU, 1.4GB object store:   1%
  |          | 400k/40.0M [00:11<18:46, 35.1k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 178; Resources: 21.0 CPU, 1.4GB object store:   1%
  |          | 400k/40.0M [00:11<18:46, 35.1k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 512.0
  MB object store; [all objects local]: : 0.00 row [00:11, ? row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 512.0
  Running Dataset: dataset_15_0. Active & requested resources: 56/70 CPU, 2.3GB/39.9GB object store: : 0.00 row [00:12, ? row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 56/70 CPU, 2.3GB/39.9GB object store: : 0.00 row [00:12, ? row/s]
  
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 176; Resources: 21.0 CPU, 1.4GB object store:   1%
  |          | 400k/40.0M [00:12<18:46, 35.1k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 176; Resources: 21.0 CPU, 1.4GB object store:   2%
  |▏         | 600k/40.0M [00:12<12:20, 53.2k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 176; Resources: 21.0 CPU, 1.4GB object store:   2%
  |▏         | 600k/40.0M [00:12<12:20, 53.2k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 2.3MB
   object store; [all objects local]: : 0.00 row [00:12, ? row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 2.3MB
   object store; [all objects local]:   0%|          | 0.00/40.0M [00:12<?, ? row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 2.3MB
   object store; [all objects local]:   2%|▏         | 600k/40.0M [00:12<13:36, 48.2k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 2.3MB
   object store; [all objects local]:   2%|▏         | 600k/40.0M [00:12<13:36, 48.2k row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 59/70 CPU, 2.1GB/39.9GB object store: : 0.00 row [00:13, ? row/s]Write: Tasks: 3; Actors: 0; Queued blocks: 0; Resources: 3.0 CPU, 768.0MB object store: : 0.00 row [00:12, ? row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 59/70 CPU, 2.1GB/39.9GB object store: : 0.00 row [00:13, ? row/s]
  
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 176; Resources: 21.0 CPU, 1.4GB object store:   2%
  |▏         | 600k/40.0M [00:13<12:20, 53.2k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 2.3MB
  Running Dataset: dataset_15_0. Active & requested resources: 59/70 CPU, 2.1GB/39.9GB object store: : 0.00 row [00:14, ? row/s]
  - Write: Tasks: 3; Actors: 0; Queued blocks: 0; Resources: 3.0 CPU, 768.0MB object store: : 0.00 row [00:13, ? row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 176; Resources: 21.0 CPU, 1.4GB object store:   2%
  |▏         | 600k/40.0M [00:14<12:20, 53.2k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 2.3MB
  Running Dataset: dataset_15_0. Active & requested resources: 59/70 CPU, 2.1GB/39.9GB object store: : 0.00 row [00:15, ? row/s]
  - Write: Tasks: 3; Actors: 0; Queued blocks: 0; Resources: 3.0 CPU, 768.0MB object store: : 0.00 row [00:14, ? row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 176; Resources: 21.0 CPU, 1.4GB object store:   2%
  |▏         | 600k/40.0M [00:15<12:20, 53.2k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 2.3MB
  Running Dataset: dataset_15_0. Active & requested resources: 59/70 CPU, 2.1GB/39.9GB object store: : 0.00 row [00:16, ? row/s]
  - Write: Tasks: 3; Actors: 0; Queued blocks: 0; Resources: 3.0 CPU, 768.0MB object store: : 0.00 row [00:15, ? row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 176; Resources: 21.0 CPU, 1.4GB object store:   2%
  |▏         | 600k/40.0M [00:16<12:20, 53.2k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 2.3MB
  Running Dataset: dataset_15_0. Active & requested resources: 59/70 CPU, 2.1GB/39.9GB object store: : 0.00 row [00:17, ? row/s]
  - Write: Tasks: 3; Actors: 0; Queued blocks: 0; Resources: 3.0 CPU, 768.0MB object store: : 0.00 row [00:16, ? row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 171; Resources: 21.0 CPU, 1.7GB object store:   2%
  |▏         | 600k/40.0M [00:17<12:20, 53.2k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 171; Resources: 21.0 CPU, 1.7GB object store:   4%
  |▍         | 1.80M/40.0M [00:17<04:46, 133k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 171; Resources: 21.0 CPU, 1.7GB object store:   4%
  |▍         | 1.80M/40.0M [00:17<04:46, 133k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 5; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.3MB
   object store; [all objects local]:   2%|▏         | 600k/40.0M [00:17<13:36, 48.2k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 5; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.3MB
   object store; [all objects local]:   2%|▏         | 800k/40.0M [00:17<14:38, 44.6k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 5; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.3MB
   object store; [all objects local]:   2%|▏         | 800k/40.0M [00:17<14:38, 44.6k row/s]
  
  - Write: Tasks: 3; Actors: 0; Queued blocks: 0; Resources: 3.0 CPU, 504.0B object store: : 0.00 row [00:17, ? row/s]
  - Write: Tasks: 3; Actors: 0; Queued blocks: 0; Resources: 3.0 CPU, 504.0B object store:   0%|          | 0.00/200 [00:17<?,
   ? row/s]
  - Write: Tasks: 3; Actors: 0; Queued blocks: 0; Resources: 3.0 CPU, 504.0B object store:   0%|          | 1.00/200 [00:17<58
  Running Dataset: dataset_15_0. Active & requested resources: 59/70 CPU, 1.9GB/39.9GB object store: : 0.00 row [00:19, ? row/s]Write: Tasks: 3; Actors: 0; Queued blocks: 0; Resources: 3.0 CPU, 504.0B object store:   0%|          | 1.00/200 [00:17<58
  Running Dataset: dataset_15_0. Active & requested resources: 59/70 CPU, 1.9GB/39.9GB object store:   0%|          | 0.00/200 [00:19<?, ? row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 59/70 CPU, 1.9GB/39.9GB object store:   0%|          | 1.00/200 [00:19<1:03:09, 19.0s/ row]
  Running Dataset: dataset_15_0. Active & requested resources: 59/70 CPU, 1.9GB/39.9GB object store:   0%|          | 1.00/200 [00:19<1:03:09, 19.0s/ row]
  
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 164; Resources: 21.0 CPU, 1.4GB object store:   4%
  |▍         | 1.80M/40.0M [00:18<04:46, 133k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 164; Resources: 21.0 CPU, 1.4GB object store:   8%
  |▊         | 3.20M/40.0M [00:18<02:16, 270k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 164; Resources: 21.0 CPU, 1.4GB object store:   8%
  |▊         | 3.20M/40.0M [00:18<02:16, 270k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 4; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 13.0M
  B object store; [all objects local]:   2%|▏         | 800k/40.0M [00:18<14:38, 44.6k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 4; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 13.0M
  B object store; [all objects local]:   7%|▋         | 2.80M/40.0M [00:18<02:45, 225k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 4; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 13.0M
  B object store; [all objects local]:   7%|▋         | 2.80M/40.0M [00:18<02:45, 225k row/s]
  
  - Write: Tasks: 13; Actors: 0; Queued blocks: 0; Resources: 13.0 CPU, 2.1KB object store:   0%|          | 1.00/200 [00:18<5
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 1.6GB/39.9GB object store:   0%|          | 1.00/200 [00:20<1:03:09, 19.0s/ row]0; Queued blocks: 0; Resources: 13.0 CPU, 2.1KB object store:   0%|          | 1.00/200 [00:18<5
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 1.6GB/39.9GB object store:   0%|          | 1.00/200 [00:20<1:03:09, 19.0s/ row]
  
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 14.5M
  B object store; [all objects local]:   7%|▋         | 2.80M/40.0M [00:19<02:45, 225k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 14.5M
  B object store; [all objects local]:  10%|▉         | 3.80M/40.0M [00:19<01:55, 313k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 14.5M
  B object store; [all objects local]:  10%|▉         | 3.80M/40.0M [00:19<01:55, 313k row/s]
  
  - Write: Tasks: 16 [backpressured:tasks]; Actors: 0; Queued blocks: 1; Resources: 16.0 CPU, 2.6KB object store:   0%|
     | 1.00/200 [00:19<58:29, 17.6s/ row]
  - Write: Tasks: 16 [backpressured:tasks]; Actors: 0; Queued blocks: 1; Resources: 16.0 CPU, 2.6KB object store:   1%|
     | 2.00/200 [00:19<28:08, 8.53s/ row]
  - ReadParquet: Tasks: 19 [backpressured:tasks]; Actors: 0; Queued blocks: 161; Resources: 19.0 CPU, 1.3GB object store:   8%
  |▊         | 3.20M/40.0M [00:19<02:16, 270k row/s]
  - ReadParquet: Tasks: 19 [backpressured:tasks]; Actors: 0; Queued blocks: 161; Resources: 19.0 CPU, 1.3GB object store:  10%
  |█         | 4.00M/40.0M [00:19<01:46, 339k row/s]
  - ReadParquet: Tasks: 19 [backpressured:tasks]; Actors: 0; Queued blocks: 161; Resources: 19.0 CPU, 1.3GB object store:  10%
  Running Dataset: dataset_15_0. Active & requested resources: 70/70 CPU, 1.3GB/39.9GB object store:   0%|          | 1.00/200 [00:21<1:03:09, 19.0s/ row]
  Running Dataset: dataset_15_0. Active & requested resources: 70/70 CPU, 1.3GB/39.9GB object store:   1%|          | 2.00/200 [00:21<30:02, 9.10s/ row]
  Running Dataset: dataset_15_0. Active & requested resources: 70/70 CPU, 1.3GB/39.9GB object store:   1%|          | 2.00/200 [00:21<30:02, 9.10s/ row]
  
  - ReadParquet: Tasks: 18 [backpressured:tasks]; Actors: 0; Queued blocks: 161; Resources: 18.0 CPU, 1.2GB object store:  10%
  |█         | 4.00M/40.0M [00:20<01:46, 339k row/s]
  - ReadParquet: Tasks: 18 [backpressured:tasks]; Actors: 0; Queued blocks: 161; Resources: 18.0 CPU, 1.2GB object store:  10%
  |█         | 4.20M/40.0M [00:20<01:53, 314k row/s]
  - ReadParquet: Tasks: 18 [backpressured:tasks]; Actors: 0; Queued blocks: 161; Resources: 18.0 CPU, 1.2GB object store:  10%
  |█         | 4.20M/40.0M [00:20<01:53, 314k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 13.7M
  B object store; [all objects local]:  10%|▉         | 3.80M/40.0M [00:20<01:55, 313k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 13.7M
  B object store; [all objects local]:  10%|█         | 4.20M/40.0M [00:20<01:50, 323k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 13.7M
  B object store; [all objects local]:  10%|█         | 4.20M/40.0M [00:20<01:50, 323k row/s]
  
  - Write: Tasks: 17 [backpressured:tasks]; Actors: 0; Queued blocks: 1; Resources: 17.0 CPU, 2.8KB object store:   1%|
     | 2.00/200 [00:20<28:08, 8.53s/ row]
  - Write: Tasks: 17 [backpressured:tasks]; Actors: 0; Queued blocks: 1; Resources: 17.0 CPU, 2.8KB object store:   2%|▏
  Running Dataset: dataset_15_0. Active & requested resources: 70/70 CPU, 1.2GB/39.9GB object store:   1%|          | 2.00/200 [00:22<30:02, 9.10s/ row]essured:tasks]; Actors: 0; Queued blocks: 1; Resources: 17.0 CPU, 2.8KB object store:   2%|▏
  Running Dataset: dataset_15_0. Active & requested resources: 70/70 CPU, 1.2GB/39.9GB object store:   2%|▏         | 3.00/200 [00:22<17:47, 5.42s/ row]
  Running Dataset: dataset_15_0. Active & requested resources: 70/70 CPU, 1.2GB/39.9GB object store:   2%|▏         | 3.00/200 [00:22<17:47, 5.42s/ row]
  
  - ReadParquet: Tasks: 18 [backpressured:tasks]; Actors: 0; Queued blocks: 161; Resources: 18.0 CPU, 1.2GB object store:  10%
  |█         | 4.20M/40.0M [00:21<01:53, 314k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 13.7M
  B object store; [all objects local]:  10%|█         | 4.20M/40.0M [00:21<01:50, 323k row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 70/70 CPU, 1.2GB/39.9GB object store:   2%|▏         | 3.00/200 [00:23<17:47, 5.42s/ row]essured:tasks]; Actors: 0; Queued blocks: 1; Resources: 17.0 CPU, 2.8KB object store:   2%|▏
     | 3.00/200 [00:21<16:47, 5.11s/ row]
  
  
  - Write: Tasks: 18; Actors: 0; Queued blocks: 0; Resources: 18.0 CPU, 3.0KB object store:   2%|▏         | 3.00/200 [00:22<1
  6:47, 5.11s/ row]
  - Write: Tasks: 18; Actors: 0; Queued blocks: 0; Resources: 18.0 CPU, 3.0KB object store:   2%|▏         | 4.00/200 [00:22<1
  2:47, 3.92s/ row]
  - ReadParquet: Tasks: 17 [backpressured:tasks]; Actors: 0; Queued blocks: 160; Resources: 17.0 CPU, 1.2GB object store:  10%
  |█         | 4.20M/40.0M [00:22<01:53, 314k row/s]
  - ReadParquet: Tasks: 17 [backpressured:tasks]; Actors: 0; Queued blocks: 160; Resources: 17.0 CPU, 1.2GB object store:  12%
  |█▏        | 4.60M/40.0M [00:22<02:08, 275k row/s]
  - ReadParquet: Tasks: 17 [backpressured:tasks]; Actors: 0; Queued blocks: 160; Resources: 17.0 CPU, 1.2GB object store:  12%
  |█▏        | 4.60M/40.0M [00:22<02:08, 275k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 13.7M
  B object store; [all objects local]:  10%|█         | 4.20M/40.0M [00:22<01:50, 323k row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 67/70 CPU, 1.2GB/39.9GB object store:   2%|▏         | 3.00/200 [00:24<17:47, 5.42s/ row]ts local]:  10%|█         | 4.20M/40.0M [00:22<01:50, 323k row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 67/70 CPU, 1.2GB/39.9GB object store:   2%|▎         | 5.00/200 [00:24<09:33, 2.94s/ row]
  Running Dataset: dataset_15_0. Active & requested resources: 67/70 CPU, 1.2GB/39.9GB object store:   2%|▎         | 5.00/200 [00:24<09:33, 2.94s/ row]
  
  - ReadParquet: Tasks: 18 [backpressured:tasks]; Actors: 0; Queued blocks: 159; Resources: 18.0 CPU, 1.2GB object store:  12%
  |█▏        | 4.60M/40.0M [00:23<02:08, 275k row/s]
  - ReadParquet: Tasks: 18 [backpressured:tasks]; Actors: 0; Queued blocks: 159; Resources: 18.0 CPU, 1.2GB object store:  12%
  |█▏        | 4.60M/40.0M [00:23<02:08, 275k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 11.4M
  B object store; [all objects local]:  10%|█         | 4.20M/40.0M [00:23<01:50, 323k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 11.4M
  B object store; [all objects local]:  12%|█▏        | 4.60M/40.0M [00:23<02:22, 248k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 11.4M
  B object store; [all objects local]:  12%|█▏        | 4.60M/40.0M [00:23<02:22, 248k row/s]
  
  - Write: Tasks: 15; Actors: 0; Queued blocks: 0; Resources: 15.0 CPU, 2.5KB object store:   2%|▏         | 4.00/200 [00:23<1
  2:47, 3.92s/ row]
  - Write: Tasks: 15; Actors: 0; Queued blocks: 0; Resources: 15.0 CPU, 2.5KB object store:   4%|▍         | 8.00/200 [00:23<0
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 1.2GB/39.9GB object store:   2%|▎         | 5.00/200 [00:25<09:33, 2.94s/ row]: 0; Queued blocks: 0; Resources: 15.0 CPU, 2.5KB object store:   4%|▍         | 8.00/200 [00:23<0
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 1.2GB/39.9GB object store:   4%|▍         | 9.00/200 [00:25<04:05, 1.29s/ row]
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 1.2GB/39.9GB object store:   4%|▍         | 9.00/200 [00:25<04:05, 1.29s/ row]
  
  
  
  - Write: Tasks: 11; Actors: 0; Queued blocks: 0; Resources: 11.0 CPU, 2.0KB object store:   4%|▍         | 8.00/200 [00:25<0
  4:25, 1.38s/ row]
  - Write: Tasks: 11; Actors: 0; Queued blocks: 0; Resources: 11.0 CPU, 2.0KB object store:   6%|▋         | 13.0/200 [00:25<0
  2:17, 1.36 row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 156; Resources: 21.0 CPU, 1.4GB object store:  12%
  |█▏        | 4.60M/40.0M [00:25<02:08, 275k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 156; Resources: 21.0 CPU, 1.4GB object store:  12%
  |█▏        | 4.60M/40.0M [00:25<02:08, 275k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 7.6MB
   object store; [all objects local]:  12%|█▏        | 4.60M/40.0M [00:25<02:22, 248k row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 66/70 CPU, 1.4GB/39.9GB object store:   4%|▍         | 9.00/200 [00:26<04:05, 1.29s/ row]s local]:  12%|█▏        | 4.60M/40.0M [00:25<02:22, 248k row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 66/70 CPU, 1.4GB/39.9GB object store:   6%|▋         | 13.0/200 [00:26<02:30, 1.24 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 66/70 CPU, 1.4GB/39.9GB object store:   6%|▋         | 13.0/200 [00:26<02:30, 1.24 row/s]
  
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 156; Resources: 21.0 CPU, 1.4GB object store:  12%
  |█▏        | 4.60M/40.0M [00:26<02:08, 275k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.1MB
   object store; [all objects local]:  12%|█▏        | 4.60M/40.0M [00:26<02:22, 248k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.1MB
   object store; [all objects local]:  12%|█▏        | 4.60M/40.0M [00:26<02:22, 248k row/s]
  
  - Write: Tasks: 8; Actors: 0; Queued blocks: 0; Resources: 8.0 CPU, 1.3KB object store:   6%|▋         | 13.0/200 [00:26<02:
  17, 1.36 row/s]
  - Write: Tasks: 8; Actors: 0; Queued blocks: 0; Resources: 8.0 CPU, 1.3KB object store:   8%|▊         | 15.0/200 [00:26<02:
  Running Dataset: dataset_15_0. Active & requested resources: 60/70 CPU, 1.4GB/39.9GB object store:   6%|▋         | 13.0/200 [00:27<02:30, 1.24 row/s] 0; Queued blocks: 0; Resources: 8.0 CPU, 1.3KB object store:   8%|▊         | 15.0/200 [00:26<02:
  Running Dataset: dataset_15_0. Active & requested resources: 60/70 CPU, 1.4GB/39.9GB object store:   8%|▊         | 16.0/200 [00:27<02:00, 1.53 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 60/70 CPU, 1.4GB/39.9GB object store:   8%|▊         | 16.0/200 [00:27<02:00, 1.53 row/s]
  
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 156; Resources: 21.0 CPU, 1.4GB object store:  12%
  |█▏        | 4.60M/40.0M [00:27<02:08, 275k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 781.2
  KB object store; [all objects local]:  12%|█▏        | 4.60M/40.0M [00:27<02:22, 248k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 781.2
  KB object store; [all objects local]:  12%|█▏        | 4.60M/40.0M [00:27<02:22, 248k row/s]
  
  - Write: Tasks: 1; Actors: 0; Queued blocks: 0; Resources: 1.0 CPU, 168.0B object store:   8%|▊         | 15.0/200 [00:27<02
  :06, 1.46 row/s]
  - Write: Tasks: 1; Actors: 0; Queued blocks: 0; Resources: 1.0 CPU, 168.0B object store:  11%|█         | 22.0/200 [00:27<01
  Running Dataset: dataset_15_0. Active & requested resources: 57/70 CPU, 1.4GB/39.9GB object store:   8%|▊         | 16.0/200 [00:28<02:00, 1.53 row/s] 0; Queued blocks: 0; Resources: 1.0 CPU, 168.0B object store:  11%|█         | 22.0/200 [00:27<01
  Running Dataset: dataset_15_0. Active & requested resources: 57/70 CPU, 1.4GB/39.9GB object store:  11%|█         | 22.0/200 [00:28<01:14, 2.39 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 57/70 CPU, 1.4GB/39.9GB object store:  11%|█         | 22.0/200 [00:28<01:14, 2.39 row/s]
  
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 156; Resources: 21.0 CPU, 1.4GB object store:  12%
  |█▏        | 4.60M/40.0M [00:28<02:08, 275k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 0.0B
  object store; [all objects local]:  12%|█▏        | 4.60M/40.0M [00:28<02:22, 248k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 0.0B
  object store; [all objects local]:  12%|█▏        | 4.60M/40.0M [00:28<02:22, 248k row/s]
  
  - Write: Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 0.0B object store:  11%|█         | 22.0/200 [00:28<01:0
  9, 2.55 row/s]
  - Write: Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 0.0B object store:  12%|█▏        | 23.0/200 [00:28<01:2
  Running Dataset: dataset_15_0. Active & requested resources: 56/70 CPU, 1.4GB/39.9GB object store:  11%|█         | 22.0/200 [00:29<01:14, 2.39 row/s] 0; Queued blocks: 0; Resources: 0.0 CPU, 0.0B object store:  12%|█▏        | 23.0/200 [00:28<01:2
  Running Dataset: dataset_15_0. Active & requested resources: 56/70 CPU, 1.4GB/39.9GB object store:  12%|█▏        | 23.0/200 [00:29<01:25, 2.07 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 56/70 CPU, 1.4GB/39.9GB object store:  12%|█▏        | 23.0/200 [00:29<01:25, 2.07 row/s]
  
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 150; Resources: 21.0 CPU, 1.7GB object store:  12%
  |█▏        | 4.60M/40.0M [00:29<02:08, 275k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 150; Resources: 21.0 CPU, 1.7GB object store:  15%
  |█▌        | 6.00M/40.0M [00:29<02:18, 245k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 150; Resources: 21.0 CPU, 1.7GB object store:  15%
  |█▌        | 6.00M/40.0M [00:29<02:18, 245k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 7; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.1MB
   object store; [all objects local]:  12%|█▏        | 4.60M/40.0M [00:29<02:22, 248k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 7; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.1MB
   object store; [all objects local]:  12%|█▏        | 4.80M/40.0M [00:29<03:59, 147k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 7; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.1MB
   object store; [all objects local]:  12%|█▏        | 4.80M/40.0M [00:29<03:59, 147k row/s]
  
  - Write: Tasks: 1; Actors: 0; Queued blocks: 0; Resources: 1.0 CPU, 168.0B object store:  12%|█▏        | 23.0/200 [00:29<01
  Running Dataset: dataset_15_0. Active & requested resources: 57/70 CPU, 1.8GB/39.9GB object store:  12%|█▏        | 23.0/200 [00:30<01:25, 2.07 row/s] 0; Queued blocks: 0; Resources: 1.0 CPU, 168.0B object store:  12%|█▏        | 23.0/200 [00:29<01
  Running Dataset: dataset_15_0. Active & requested resources: 57/70 CPU, 1.8GB/39.9GB object store:  12%|█▏        | 23.0/200 [00:30<01:25, 2.07 row/s]
  
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 144; Resources: 21.0 CPU, 1.5GB object store:  15%
  |█▌        | 6.00M/40.0M [00:30<02:18, 245k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 144; Resources: 21.0 CPU, 1.5GB object store:  18%
  |█▊        | 7.00M/40.0M [00:30<01:40, 330k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 144; Resources: 21.0 CPU, 1.5GB object store:  18%
  |█▊        | 7.00M/40.0M [00:30<01:40, 330k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 9.2MB
   object store; [all objects local]:  12%|█▏        | 4.80M/40.0M [00:30<03:59, 147k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 9.2MB
   object store; [all objects local]:  16%|█▋        | 6.60M/40.0M [00:30<01:39, 336k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 9.2MB
   object store; [all objects local]:  16%|█▋        | 6.60M/40.0M [00:30<01:39, 336k row/s]
  
  - Write: Tasks: 10; Actors: 0; Queued blocks: 0; Resources: 10.0 CPU, 1.6KB object store:  12%|█▏        | 23.0/200 [00:30<0
  Running Dataset: dataset_15_0. Active & requested resources: 66/70 CPU, 1.5GB/39.9GB object store:  12%|█▏        | 23.0/200 [00:31<01:25, 2.07 row/s]: 0; Queued blocks: 0; Resources: 10.0 CPU, 1.6KB object store:  12%|█▏        | 23.0/200 [00:30<0
  Running Dataset: dataset_15_0. Active & requested resources: 66/70 CPU, 1.5GB/39.9GB object store:  12%|█▏        | 23.0/200 [00:31<01:25, 2.07 row/s]
  
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 144; Resources: 21.0 CPU, 1.4GB object store:  18%
  |█▊        | 7.00M/40.0M [00:31<01:40, 330k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 144; Resources: 21.0 CPU, 1.4GB object store:  18%
  |█▊        | 7.00M/40.0M [00:31<01:40, 330k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 9.2MB
   object store; [all objects local]:  16%|█▋        | 6.60M/40.0M [00:31<01:39, 336k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 9.2MB
   object store; [all objects local]:  18%|█▊        | 7.00M/40.0M [00:31<01:36, 343k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 9.2MB
   object store; [all objects local]:  18%|█▊        | 7.00M/40.0M [00:31<01:36, 343k row/s]
  
  - Write: Tasks: 12; Actors: 0; Queued blocks: 0; Resources: 12.0 CPU, 2.0KB object store:  12%|█▏        | 23.0/200 [00:31<0
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 1.4GB/39.9GB object store:  12%|█▏        | 23.0/200 [00:32<01:25, 2.07 row/s]: 0; Queued blocks: 0; Resources: 12.0 CPU, 2.0KB object store:  12%|█▏        | 23.0/200 [00:31<0
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 1.4GB/39.9GB object store:  12%|█▏        | 23.0/200 [00:32<01:25, 2.07 row/s]
  
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 144; Resources: 21.0 CPU, 1.4GB object store:  18%
  |█▊        | 7.00M/40.0M [00:32<01:40, 330k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 9.2MB
   object store; [all objects local]:  18%|█▊        | 7.00M/40.0M [00:32<01:36, 343k row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 1.4GB/39.9GB object store:  12%|█▏        | 23.0/200 [00:33<01:25, 2.07 row/s]: 0; Queued blocks: 0; Resources: 12.0 CPU, 2.0KB object store:  12%|█▏        | 23.0/200 [00:32<0
  1:20, 2.19 row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 142; Resources: 21.0 CPU, 1.5GB object store:  18%
  |█▊        | 7.00M/40.0M [00:33<01:40, 330k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 142; Resources: 21.0 CPU, 1.5GB object store:  18%
  |█▊        | 7.40M/40.0M [00:33<02:04, 261k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 142; Resources: 21.0 CPU, 1.5GB object store:  18%
  |█▊        | 7.40M/40.0M [00:33<02:04, 261k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 10.7M
  B object store; [all objects local]:  18%|█▊        | 7.00M/40.0M [00:33<01:36, 343k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 10.7M
  B object store; [all objects local]:  18%|█▊        | 7.00M/40.0M [00:33<01:36, 343k row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 69/70 CPU, 1.4GB/39.9GB object store:  12%|█▏        | 23.0/200 [00:34<01:25, 2.07 row/s]: 0; Queued blocks: 0; Resources: 12.0 CPU, 2.0KB object store:  12%|█▏        | 23.0/200 [00:33<0
  Running Dataset: dataset_15_0. Active & requested resources: 69/70 CPU, 1.4GB/39.9GB object store:  12%|█▏        | 23.0/200 [00:34<01:25, 2.07 row/s]
  
  
  
  - Write: Tasks: 10; Actors: 0; Queued blocks: 0; Resources: 10.0 CPU, 1.8KB object store:  12%|█▏        | 23.0/200 [00:34<0
  1:20, 2.19 row/s]
  - Write: Tasks: 10; Actors: 0; Queued blocks: 0; Resources: 10.0 CPU, 1.8KB object store:  14%|█▍        | 28.0/200 [00:34<0
  2:16, 1.26 row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 141; Resources: 21.0 CPU, 1.4GB object store:  18%
  |█▊        | 7.40M/40.0M [00:34<02:04, 261k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 141; Resources: 21.0 CPU, 1.4GB object store:  19%
  |█▉        | 7.60M/40.0M [00:34<02:08, 252k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 141; Resources: 21.0 CPU, 1.4GB object store:  19%
  |█▉        | 7.60M/40.0M [00:34<02:08, 252k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 7.6MB
   object store; [all objects local]:  18%|█▊        | 7.00M/40.0M [00:34<01:36, 343k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 7.6MB
   object store; [all objects local]:  18%|█▊        | 7.40M/40.0M [00:34<02:05, 260k row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 65/70 CPU, 1.4GB/39.9GB object store:  12%|█▏        | 23.0/200 [00:35<01:25, 2.07 row/s]s local]:  18%|█▊        | 7.40M/40.0M [00:34<02:05, 260k row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 65/70 CPU, 1.4GB/39.9GB object store:  14%|█▍        | 29.0/200 [00:35<02:08, 1.33 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 65/70 CPU, 1.4GB/39.9GB object store:  14%|█▍        | 29.0/200 [00:35<02:08, 1.33 row/s]
  
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 139; Resources: 21.0 CPU, 1.4GB object store:  19%
  |█▉        | 7.60M/40.0M [00:35<02:08, 252k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 139; Resources: 21.0 CPU, 1.4GB object store:  20%
  |██        | 8.00M/40.0M [00:35<01:57, 273k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 139; Resources: 21.0 CPU, 1.4GB object store:  20%
  |██        | 8.00M/40.0M [00:35<01:57, 273k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 3.8MB
   object store; [all objects local]:  18%|█▊        | 7.40M/40.0M [00:35<02:05, 260k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 3.8MB
   object store; [all objects local]:  20%|█▉        | 7.80M/40.0M [00:35<01:55, 279k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 3.8MB
   object store; [all objects local]:  20%|█▉        | 7.80M/40.0M [00:35<01:55, 279k row/s]
  
  - Write: Tasks: 4; Actors: 0; Queued blocks: 0; Resources: 4.0 CPU, 672.0B object store:  14%|█▍        | 28.0/200 [00:35<02
  :16, 1.26 row/s]
  - Write: Tasks: 4; Actors: 0; Queued blocks: 0; Resources: 4.0 CPU, 672.0B object store:  18%|█▊        | 35.0/200 [00:35<01
  Running Dataset: dataset_15_0. Active & requested resources: 60/70 CPU, 1.5GB/39.9GB object store:  14%|█▍        | 29.0/200 [00:36<02:08, 1.33 row/s] 0; Queued blocks: 0; Resources: 4.0 CPU, 672.0B object store:  18%|█▊        | 35.0/200 [00:35<01
  Running Dataset: dataset_15_0. Active & requested resources: 60/70 CPU, 1.5GB/39.9GB object store:  18%|█▊        | 35.0/200 [00:36<01:25, 1.92 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 60/70 CPU, 1.5GB/39.9GB object store:  18%|█▊        | 35.0/200 [00:36<01:25, 1.92 row/s]
  
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 136; Resources: 21.0 CPU, 1.5GB object store:  20%
  |██        | 8.00M/40.0M [00:36<01:57, 273k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 136; Resources: 21.0 CPU, 1.5GB object store:  22%
  |██▏       | 8.60M/40.0M [00:36<01:35, 330k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 136; Resources: 21.0 CPU, 1.5GB object store:  22%
  |██▏       | 8.60M/40.0M [00:36<01:35, 330k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.1MB
   object store; [all objects local]:  20%|█▉        | 7.80M/40.0M [00:36<01:55, 279k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.1MB
   object store; [all objects local]:  20%|██        | 8.20M/40.0M [00:36<01:46, 297k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.1MB
   object store; [all objects local]:  20%|██        | 8.20M/40.0M [00:36<01:46, 297k row/s]
  
  - Write: Tasks: 6; Actors: 0; Queued blocks: 0; Resources: 6.0 CPU, 1008.0B object store:  18%|█▊        | 35.0/200 [00:36<0
  Running Dataset: dataset_15_0. Active & requested resources: 62/70 CPU, 1.4GB/39.9GB object store:  18%|█▊        | 35.0/200 [00:37<01:25, 1.92 row/s] 0; Queued blocks: 0; Resources: 6.0 CPU, 1008.0B object store:  18%|█▊        | 35.0/200 [00:36<0
  Running Dataset: dataset_15_0. Active & requested resources: 62/70 CPU, 1.4GB/39.9GB object store:  18%|█▊        | 35.0/200 [00:37<01:25, 1.92 row/s]
  
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 136; Resources: 21.0 CPU, 1.4GB object store:  22%
  |██▏       | 8.60M/40.0M [00:37<01:35, 330k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 136; Resources: 21.0 CPU, 1.4GB object store:  22%
  |██▏       | 8.60M/40.0M [00:37<01:35, 330k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.3MB
   object store; [all objects local]:  20%|██        | 8.20M/40.0M [00:37<01:46, 297k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.3MB
   object store; [all objects local]:  22%|██▏       | 8.60M/40.0M [00:37<01:39, 315k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.3MB
   object store; [all objects local]:  22%|██▏       | 8.60M/40.0M [00:37<01:39, 315k row/s]
  
  - Write: Tasks: 7; Actors: 0; Queued blocks: 0; Resources: 7.0 CPU, 1.1KB object store:  18%|█▊        | 35.0/200 [00:37<01:
  22, 2.00 row/s]
  - Write: Tasks: 7; Actors: 0; Queued blocks: 0; Resources: 7.0 CPU, 1.1KB object store:  18%|█▊        | 36.0/200 [00:37<01:
  Running Dataset: dataset_15_0. Active & requested resources: 63/70 CPU, 1.4GB/39.9GB object store:  18%|█▊        | 35.0/200 [00:38<01:25, 1.92 row/s] 0; Queued blocks: 0; Resources: 7.0 CPU, 1.1KB object store:  18%|█▊        | 36.0/200 [00:37<01:
  Running Dataset: dataset_15_0. Active & requested resources: 63/70 CPU, 1.4GB/39.9GB object store:  18%|█▊        | 36.0/200 [00:38<01:47, 1.52 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 63/70 CPU, 1.4GB/39.9GB object store:  18%|█▊        | 36.0/200 [00:38<01:47, 1.52 row/s]
  
  
  
  - Write: Tasks: 6; Actors: 0; Queued blocks: 0; Resources: 6.0 CPU, 1008.0B object store:  18%|█▊        | 36.0/200 [00:38<0
  1:44, 1.57 row/s]
  - Write: Tasks: 6; Actors: 0; Queued blocks: 0; Resources: 6.0 CPU, 1008.0B object store:  19%|█▉        | 38.0/200 [00:38<0
  1:39, 1.63 row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 136; Resources: 21.0 CPU, 1.4GB object store:  22%
  |██▏       | 8.60M/40.0M [00:38<01:35, 330k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 3.1MB
   object store; [all objects local]:  22%|██▏       | 8.60M/40.0M [00:38<01:39, 315k row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 60/70 CPU, 1.4GB/39.9GB object store:  18%|█▊        | 36.0/200 [00:40<01:47, 1.52 row/s]s local]:  22%|██▏       | 8.60M/40.0M [00:38<01:39, 315k row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 60/70 CPU, 1.4GB/39.9GB object store:  20%|█▉        | 39.0/200 [00:40<01:32, 1.75 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 60/70 CPU, 1.4GB/39.9GB object store:  20%|█▉        | 39.0/200 [00:40<01:32, 1.75 row/s]
  
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 136; Resources: 21.0 CPU, 1.4GB object store:  22%
  |██▏       | 8.60M/40.0M [00:39<01:35, 330k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 3.1MB
   object store; [all objects local]:  22%|██▏       | 8.60M/40.0M [00:39<01:39, 315k row/s]
  
  - Write: Tasks: 4; Actors: 0; Queued blocks: 0; Resources: 4.0 CPU, 672.0B object store:  19%|█▉        | 38.0/200 [00:39<01
  :39, 1.63 row/s]
  - Write: Tasks: 4; Actors: 0; Queued blocks: 0; Resources: 4.0 CPU, 672.0B object store:  20%|█▉        | 39.0/200 [00:39<01
  Running Dataset: dataset_15_0. Active & requested resources: 59/70 CPU, 1.4GB/39.9GB object store:  20%|█▉        | 39.0/200 [00:41<01:32, 1.75 row/s] 0; Queued blocks: 0; Resources: 4.0 CPU, 672.0B object store:  20%|█▉        | 39.0/200 [00:39<01
  Running Dataset: dataset_15_0. Active & requested resources: 59/70 CPU, 1.4GB/39.9GB object store:  20%|█▉        | 39.0/200 [00:41<01:32, 1.75 row/s]
  
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 131; Resources: 21.0 CPU, 1.6GB object store:  22%
  |██▏       | 8.60M/40.0M [00:40<01:35, 330k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 131; Resources: 21.0 CPU, 1.6GB object store:  24%
  |██▍       | 9.80M/40.0M [00:40<01:38, 308k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 131; Resources: 21.0 CPU, 1.6GB object store:  24%
  |██▍       | 9.80M/40.0M [00:40<01:38, 308k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 5; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.1MB
   object store; [all objects local]:  22%|██▏       | 8.60M/40.0M [00:40<01:39, 315k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 5; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.1MB
   object store; [all objects local]:  22%|██▏       | 8.80M/40.0M [00:40<02:38, 197k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 5; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.1MB
   object store; [all objects local]:  22%|██▏       | 8.80M/40.0M [00:40<02:38, 197k row/s]
  
  - Write: Tasks: 3; Actors: 0; Queued blocks: 0; Resources: 3.0 CPU, 504.0B object store:  20%|█▉        | 39.0/200 [00:40<01
  :46, 1.50 row/s]
  - Write: Tasks: 3; Actors: 0; Queued blocks: 0; Resources: 3.0 CPU, 504.0B object store:  20%|██        | 41.0/200 [00:40<01
  Running Dataset: dataset_15_0. Active & requested resources: 58/70 CPU, 1.7GB/39.9GB object store:  20%|█▉        | 39.0/200 [00:42<01:32, 1.75 row/s] 0; Queued blocks: 0; Resources: 3.0 CPU, 504.0B object store:  20%|██        | 41.0/200 [00:40<01
  Running Dataset: dataset_15_0. Active & requested resources: 58/70 CPU, 1.7GB/39.9GB object store:  21%|██        | 42.0/200 [00:42<01:36, 1.64 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 58/70 CPU, 1.7GB/39.9GB object store:  21%|██        | 42.0/200 [00:42<01:36, 1.64 row/s]
  
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 126; Resources: 21.0 CPU, 1.4GB object store:  24%
  |██▍       | 9.80M/40.0M [00:41<01:38, 308k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 126; Resources: 21.0 CPU, 1.4GB object store:  25%
  |██▍       | 9.80M/39.6M [00:41<01:36, 308k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 126; Resources: 21.0 CPU, 1.4GB object store:  26%
  |██▋       | 10.5M/39.6M [00:41<01:20, 363k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 126; Resources: 21.0 CPU, 1.4GB object store:  26%
  |██▋       | 10.5M/39.6M [00:41<01:20, 363k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 8.0MB
   object store; [all objects local]:  22%|██▏       | 8.80M/40.0M [00:41<02:38, 197k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 8.0MB
   object store; [all objects local]:  22%|██▏       | 8.80M/39.6M [00:41<02:36, 197k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 8.0MB
   object store; [all objects local]:  26%|██▌       | 10.3M/39.6M [00:41<01:09, 421k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 8.0MB
   object store; [all objects local]:  26%|██▌       | 10.3M/39.6M [00:41<01:09, 421k row/s]
  
  - Write: Tasks: 10; Actors: 0; Queued blocks: 0; Resources: 10.0 CPU, 1.6KB object store:  20%|██        | 41.0/200 [00:41<0
  1:40, 1.59 row/s]
  - Write: Tasks: 10; Actors: 0; Queued blocks: 0; Resources: 10.0 CPU, 1.6KB object store:  21%|██        | 42.0/200 [00:41<0
  Running Dataset: dataset_15_0. Active & requested resources: 67/70 CPU, 1.4GB/39.9GB object store:  21%|██        | 42.0/200 [00:43<01:36, 1.64 row/s]: 0; Queued blocks: 0; Resources: 10.0 CPU, 1.6KB object store:  21%|██        | 42.0/200 [00:41<0
  Running Dataset: dataset_15_0. Active & requested resources: 67/70 CPU, 1.4GB/39.9GB object store:  21%|██        | 42.0/200 [00:43<01:36, 1.64 row/s]
  
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 123; Resources: 21.0 CPU, 1.4GB object store:  26%
  |██▋       | 10.5M/39.6M [00:42<01:20, 363k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 123; Resources: 21.0 CPU, 1.4GB object store:  27%
  |██▋       | 10.5M/38.9M [00:42<01:18, 363k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 123; Resources: 21.0 CPU, 1.4GB object store:  28%
  |██▊       | 10.9M/38.9M [00:42<01:16, 366k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 123; Resources: 21.0 CPU, 1.4GB object store:  28%
  |██▊       | 10.9M/38.9M [00:42<01:16, 366k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 8.8MB
   object store; [all objects local]:  26%|██▌       | 10.3M/39.6M [00:42<01:09, 421k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 8.8MB
   object store; [all objects local]:  26%|██▋       | 10.3M/38.9M [00:42<01:07, 421k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 8.8MB
   object store; [all objects local]:  27%|██▋       | 10.7M/38.9M [00:42<01:08, 412k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 8.8MB
   object store; [all objects local]:  27%|██▋       | 10.7M/38.9M [00:42<01:08, 412k row/s]
  
  - Write: Tasks: 12; Actors: 0; Queued blocks: 0; Resources: 12.0 CPU, 2.0KB object store:  21%|██        | 42.0/200 [00:42<0
  1:50, 1.43 row/s]
  - Write: Tasks: 12; Actors: 0; Queued blocks: 0; Resources: 12.0 CPU, 2.0KB object store:  22%|██▏       | 43.0/200 [00:42<0
  Running Dataset: dataset_15_0. Active & requested resources: 69/70 CPU, 1.3GB/39.9GB object store:  21%|██        | 42.0/200 [00:44<01:36, 1.64 row/s]: 0; Queued blocks: 0; Resources: 12.0 CPU, 2.0KB object store:  22%|██▏       | 43.0/200 [00:42<0
  Running Dataset: dataset_15_0. Active & requested resources: 69/70 CPU, 1.3GB/39.9GB object store:  22%|██▏       | 43.0/200 [00:44<02:03, 1.27 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 69/70 CPU, 1.3GB/39.9GB object store:  22%|██▏       | 43.0/200 [00:44<02:03, 1.27 row/s]
  
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 123; Resources: 21.0 CPU, 1.3GB object store:  28%
  |██▊       | 10.9M/38.9M [00:43<01:16, 366k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 123; Resources: 21.0 CPU, 1.3GB object store:  28%
  |██▊       | 10.9M/38.9M [00:43<01:16, 366k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 8.8MB
   object store; [all objects local]:  27%|██▋       | 10.7M/38.9M [00:43<01:08, 412k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 8.8MB
   object store; [all objects local]:  27%|██▋       | 10.7M/38.9M [00:43<01:08, 412k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 8.8MB
   object store; [all objects local]:  28%|██▊       | 10.9M/38.9M [00:43<01:17, 362k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 8.8MB
   object store; [all objects local]:  28%|██▊       | 10.9M/38.9M [00:43<01:17, 362k row/s]
  
  - Write: Tasks: 13; Actors: 0; Queued blocks: 0; Resources: 13.0 CPU, 2.1KB object store:  22%|██▏       | 43.0/200 [00:43<0
  Running Dataset: dataset_15_0. Active & requested resources: 69/70 CPU, 1.3GB/39.9GB object store:  22%|██▏       | 43.0/200 [00:45<02:03, 1.27 row/s]: 0; Queued blocks: 0; Resources: 13.0 CPU, 2.1KB object store:  22%|██▏       | 43.0/200 [00:43<0
  2:00, 1.31 row/s]
  - ReadParquet: Tasks: 20 [backpressured:tasks]; Actors: 0; Queued blocks: 122; Resources: 20.0 CPU, 1.4GB object store:  28%
  |██▊       | 10.9M/38.9M [00:44<01:16, 366k row/s]
  - ReadParquet: Tasks: 20 [backpressured:tasks]; Actors: 0; Queued blocks: 122; Resources: 20.0 CPU, 1.4GB object store:  28%
  |██▊       | 10.9M/39.0M [00:44<01:16, 366k row/s]
  - ReadParquet: Tasks: 20 [backpressured:tasks]; Actors: 0; Queued blocks: 122; Resources: 20.0 CPU, 1.4GB object store:  29%
  |██▉       | 11.3M/39.0M [00:44<01:30, 307k row/s]
  - ReadParquet: Tasks: 20 [backpressured:tasks]; Actors: 0; Queued blocks: 122; Resources: 20.0 CPU, 1.4GB object store:  29%
  |██▉       | 11.3M/39.0M [00:44<01:30, 307k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 10.3M
  B object store; [all objects local]:  28%|██▊       | 10.9M/38.9M [00:44<01:17, 362k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 10.3M
  B object store; [all objects local]:  28%|██▊       | 10.9M/38.9M [00:44<01:17, 362k row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 66/70 CPU, 1.4GB/39.9GB object store:  22%|██▏       | 43.0/200 [00:46<02:03, 1.27 row/s]: 0; Queued blocks: 0; Resources: 13.0 CPU, 2.1KB object store:  22%|██▏       | 43.0/200 [00:44<0
  Running Dataset: dataset_15_0. Active & requested resources: 66/70 CPU, 1.4GB/39.9GB object store:  22%|██▏       | 43.0/200 [00:46<02:03, 1.27 row/s]
  
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 116; Resources: 21.0 CPU, 1.4GB object store:  29%
  |██▉       | 11.3M/39.0M [00:46<01:30, 307k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 116; Resources: 21.0 CPU, 1.4GB object store:  30%
  |██▉       | 11.3M/37.8M [00:46<01:26, 307k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 116; Resources: 21.0 CPU, 1.4GB object store:  32%
  |███▏      | 12.2M/37.8M [00:46<01:02, 413k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 116; Resources: 21.0 CPU, 1.4GB object store:  32%
  |███▏      | 12.2M/37.8M [00:46<01:02, 413k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 4; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 10.2M
  B object store; [all objects local]:  28%|██▊       | 10.9M/38.9M [00:46<01:17, 362k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 4; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 10.2M
  B object store; [all objects local]:  28%|██▊       | 10.9M/38.7M [00:46<01:16, 362k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 4; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 10.2M
  B object store; [all objects local]:  30%|███       | 11.8M/38.7M [00:46<01:09, 386k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 4; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 10.2M
  B object store; [all objects local]:  30%|███       | 11.8M/38.7M [00:46<01:09, 386k row/s]
  
  - Write: Tasks: 11; Actors: 0; Queued blocks: 0; Resources: 11.0 CPU, 1.8KB object store:  22%|██▏       | 43.0/200 [00:46<0
  2:00, 1.31 row/s]
  - Write: Tasks: 11; Actors: 0; Queued blocks: 0; Resources: 11.0 CPU, 1.8KB object store:  25%|██▌       | 50.0/200 [00:46<0
  Running Dataset: dataset_15_0. Active & requested resources: 65/70 CPU, 1.4GB/39.9GB object store:  22%|██▏       | 43.0/200 [00:47<02:03, 1.27 row/s]: 0; Queued blocks: 0; Resources: 11.0 CPU, 1.8KB object store:  25%|██▌       | 50.0/200 [00:46<0
  Running Dataset: dataset_15_0. Active & requested resources: 65/70 CPU, 1.4GB/39.9GB object store:  25%|██▌       | 50.0/200 [00:47<01:29, 1.67 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 65/70 CPU, 1.4GB/39.9GB object store:  25%|██▌       | 50.0/200 [00:47<01:29, 1.67 row/s]
  
  - ReadParquet: Tasks: 18 [backpressured:tasks]; Actors: 0; Queued blocks: 111; Resources: 18.0 CPU, 1.1GB object store:  32%
  |███▏      | 12.2M/37.8M [00:47<01:02, 413k row/s]
  - ReadParquet: Tasks: 18 [backpressured:tasks]; Actors: 0; Queued blocks: 111; Resources: 18.0 CPU, 1.1GB object store:  34%
  |███▎      | 12.2M/36.3M [00:47<00:58, 413k row/s]
  - ReadParquet: Tasks: 18 [backpressured:tasks]; Actors: 0; Queued blocks: 111; Resources: 18.0 CPU, 1.1GB object store:  36%
  |███▌      | 12.9M/36.3M [00:47<00:50, 464k row/s]
  - ReadParquet: Tasks: 18 [backpressured:tasks]; Actors: 0; Queued blocks: 111; Resources: 18.0 CPU, 1.1GB object store:  36%
  |███▌      | 12.9M/36.3M [00:47<00:50, 464k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 9.8MB
   object store; [all objects local]:  30%|███       | 11.8M/38.7M [00:47<01:09, 386k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 9.8MB
   object store; [all objects local]:  32%|███▏      | 11.8M/36.8M [00:47<01:04, 386k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 9.8MB
   object store; [all objects local]:  35%|███▍      | 12.7M/36.8M [00:47<00:49, 486k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 9.8MB
   object store; [all objects local]:  35%|███▍      | 12.7M/36.8M [00:47<00:49, 486k row/s]
  
  - Write: Tasks: 16; Actors: 0; Queued blocks: 0; Resources: 16.0 CPU, 2.6KB object store:  25%|██▌       | 50.0/200 [00:47<0
  1:24, 1.77 row/s]
  - Write: Tasks: 16; Actors: 0; Queued blocks: 0; Resources: 16.0 CPU, 2.6KB object store:  26%|██▋       | 53.0/200 [00:47<0
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 1.1GB/39.9GB object store:  25%|██▌       | 50.0/200 [00:48<01:29, 1.67 row/s]: 0; Queued blocks: 0; Resources: 16.0 CPU, 2.6KB object store:  26%|██▋       | 53.0/200 [00:47<0
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 1.1GB/39.9GB object store:  26%|██▋       | 53.0/200 [00:48<01:19, 1.86 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 1.1GB/39.9GB object store:  26%|██▋       | 53.0/200 [00:48<01:19, 1.86 row/s]
  
  - ReadParquet: Tasks: 18 [backpressured:tasks]; Actors: 0; Queued blocks: 107; Resources: 18.0 CPU, 1.2GB object store:  36%
  |███▌      | 12.9M/36.3M [00:48<00:50, 464k row/s]
  - ReadParquet: Tasks: 18 [backpressured:tasks]; Actors: 0; Queued blocks: 107; Resources: 18.0 CPU, 1.2GB object store:  36%
  |███▌      | 12.9M/36.0M [00:48<00:49, 464k row/s]
  - ReadParquet: Tasks: 18 [backpressured:tasks]; Actors: 0; Queued blocks: 107; Resources: 18.0 CPU, 1.2GB object store:  38%
  |███▊      | 13.5M/36.0M [00:48<00:46, 487k row/s]
  - ReadParquet: Tasks: 18 [backpressured:tasks]; Actors: 0; Queued blocks: 107; Resources: 18.0 CPU, 1.2GB object store:  38%
  |███▊      | 13.5M/36.0M [00:48<00:46, 487k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 3; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 10.1M
  B object store; [all objects local]:  35%|███▍      | 12.7M/36.8M [00:48<00:49, 486k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 3; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 10.1M
  B object store; [all objects local]:  35%|███▌      | 12.7M/36.1M [00:48<00:48, 486k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 3; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 10.1M
  B object store; [all objects local]:  36%|███▌      | 13.0M/36.1M [00:48<00:52, 437k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 3; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 10.1M
  B object store; [all objects local]:  36%|███▌      | 13.0M/36.1M [00:48<00:52, 437k row/s]
  
  - Write: Tasks: 16; Actors: 0; Queued blocks: 0; Resources: 16.0 CPU, 2.6KB object store:  28%|██▊       | 56.0/200 [00:48<0
  Running Dataset: dataset_15_0. Active & requested resources: 69/70 CPU, 1.2GB/39.9GB object store:  26%|██▋       | 53.0/200 [00:49<01:19, 1.86 row/s]: 0; Queued blocks: 0; Resources: 16.0 CPU, 2.6KB object store:  28%|██▊       | 56.0/200 [00:48<0
  Running Dataset: dataset_15_0. Active & requested resources: 69/70 CPU, 1.2GB/39.9GB object store:  28%|██▊       | 56.0/200 [00:49<01:10, 2.04 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 69/70 CPU, 1.2GB/39.9GB object store:  28%|██▊       | 56.0/200 [00:49<01:10, 2.04 row/s]
  
  
  
  - Write: Tasks: 16; Actors: 0; Queued blocks: 0; Resources: 16.0 CPU, 2.8KB object store:  28%|██▊       | 56.0/200 [00:49<0
  1:06, 2.16 row/s]
  - Write: Tasks: 16; Actors: 0; Queued blocks: 0; Resources: 16.0 CPU, 2.8KB object store:  30%|███       | 60.0/200 [00:49<0
  0:54, 2.56 row/s]
  - ReadParquet: Tasks: 18 [backpressured:tasks]; Actors: 0; Queued blocks: 107; Resources: 18.0 CPU, 1.1GB object store:  38%
  |███▊      | 13.5M/36.0M [00:49<00:46, 487k row/s]
  - ReadParquet: Tasks: 18 [backpressured:tasks]; Actors: 0; Queued blocks: 107; Resources: 18.0 CPU, 1.1GB object store:  38%
  |███▊      | 13.5M/36.0M [00:49<00:46, 487k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 7.6MB
   object store; [all objects local]:  36%|███▌      | 13.0M/36.1M [00:49<00:52, 437k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 7.6MB
   object store; [all objects local]:  36%|███▌      | 13.0M/36.0M [00:49<00:52, 437k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 7.6MB
   object store; [all objects local]:  38%|███▊      | 13.5M/36.0M [00:49<00:50, 447k row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 67/70 CPU, 1.1GB/39.9GB object store:  28%|██▊       | 56.0/200 [00:50<01:10, 2.04 row/s]s local]:  38%|███▊      | 13.5M/36.0M [00:49<00:50, 447k row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 67/70 CPU, 1.1GB/39.9GB object store:  30%|███       | 60.0/200 [00:50<00:58, 2.41 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 67/70 CPU, 1.1GB/39.9GB object store:  30%|███       | 60.0/200 [00:50<00:58, 2.41 row/s]
  
  
  
  - Write: Tasks: 12; Actors: 0; Queued blocks: 0; Resources: 12.0 CPU, 2.0KB object store:  30%|███       | 60.0/200 [00:50<0
  0:54, 2.56 row/s]
  - Write: Tasks: 12; Actors: 0; Queued blocks: 0; Resources: 12.0 CPU, 2.0KB object store:  32%|███▏      | 64.0/200 [00:50<0
  0:47, 2.88 row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 104; Resources: 21.0 CPU, 1.2GB object store:  38%
  |███▊      | 13.5M/36.0M [00:50<00:46, 487k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 104; Resources: 21.0 CPU, 1.2GB object store:  38%
  |███▊      | 13.5M/36.0M [00:50<00:46, 487k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.3MB
   object store; [all objects local]:  38%|███▊      | 13.5M/36.0M [00:50<00:50, 447k row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 65/70 CPU, 1.2GB/39.9GB object store:  30%|███       | 60.0/200 [00:51<00:58, 2.41 row/s]s local]:  38%|███▊      | 13.5M/36.0M [00:50<00:50, 447k row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 65/70 CPU, 1.2GB/39.9GB object store:  32%|███▎      | 65.0/200 [00:51<00:45, 2.96 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 65/70 CPU, 1.2GB/39.9GB object store:  32%|███▎      | 65.0/200 [00:51<00:45, 2.96 row/s]
  
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 4; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.9MB
   object store; [all objects local]:  38%|███▊      | 13.5M/36.0M [00:51<00:50, 447k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 4; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.9MB
   object store; [all objects local]:  38%|███▊      | 13.5M/35.2M [00:51<00:48, 447k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 4; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.9MB
   object store; [all objects local]:  39%|███▉      | 13.8M/35.2M [00:51<01:07, 319k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 4; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.9MB
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 98; Resources: 21.0 CPU, 1.3GB object store:  38%|
  ███▊      | 13.5M/36.0M [00:51<00:46, 487k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 98; Resources: 21.0 CPU, 1.3GB object store:  39%|
  ███▉      | 13.5M/34.8M [00:51<00:43, 487k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 98; Resources: 21.0 CPU, 1.3GB object store:  40%|
  ████      | 14.1M/34.8M [00:51<01:01, 337k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 98; Resources: 21.0 CPU, 1.3GB object store:  40%|
  ████      | 14.1M/34.8M [00:51<01:01, 337k row/s]
  
  
  - Write: Tasks: 10; Actors: 0; Queued blocks: 0; Resources: 10.0 CPU, 1.6KB object store:  32%|███▏      | 64.0/200 [00:51<0
  0:47, 2.88 row/s]
  - Write: Tasks: 10; Actors: 0; Queued blocks: 0; Resources: 10.0 CPU, 1.6KB object store:  34%|███▍      | 69.0/200 [00:51<0
  Running Dataset: dataset_15_0. Active & requested resources: 63/70 CPU, 1.2GB/39.9GB object store:  32%|███▎      | 65.0/200 [00:52<00:45, 2.96 row/s]: 0; Queued blocks: 0; Resources: 10.0 CPU, 1.6KB object store:  34%|███▍      | 69.0/200 [00:51<0
  Running Dataset: dataset_15_0. Active & requested resources: 63/70 CPU, 1.2GB/39.9GB object store:  34%|███▍      | 69.0/200 [00:52<00:41, 3.15 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 63/70 CPU, 1.2GB/39.9GB object store:  34%|███▍      | 69.0/200 [00:52<00:41, 3.15 row/s]
  
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 92; Resources: 21.0 CPU, 1.2GB object store:  40%|
  ████      | 14.1M/34.8M [00:52<01:01, 337k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 92; Resources: 21.0 CPU, 1.2GB object store:  42%|
  ████▏     | 14.1M/33.6M [00:52<00:57, 337k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 92; Resources: 21.0 CPU, 1.2GB object store:  44%|
  ████▍     | 14.8M/33.6M [00:52<00:47, 398k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 92; Resources: 21.0 CPU, 1.2GB object store:  44%|
  ████▍     | 14.8M/33.6M [00:52<00:47, 398k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.5MB
   object store; [all objects local]:  39%|███▉      | 13.8M/35.2M [00:52<01:07, 319k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.5MB
   object store; [all objects local]:  41%|████      | 13.8M/34.0M [00:52<01:03, 319k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.5MB
   object store; [all objects local]:  43%|████▎     | 14.6M/34.0M [00:52<00:46, 416k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.5MB
   object store; [all objects local]:  43%|████▎     | 14.6M/34.0M [00:52<00:46, 416k row/s]
  
  - Write: Tasks: 11; Actors: 0; Queued blocks: 0; Resources: 11.0 CPU, 1.8KB object store:  34%|███▍      | 69.0/200 [00:52<0
  0:39, 3.35 row/s]
  - Write: Tasks: 11; Actors: 0; Queued blocks: 0; Resources: 11.0 CPU, 1.8KB object store:  38%|███▊      | 75.0/200 [00:52<0
  Running Dataset: dataset_15_0. Active & requested resources: 67/70 CPU, 1.2GB/39.9GB object store:  34%|███▍      | 69.0/200 [00:53<00:41, 3.15 row/s]: 0; Queued blocks: 0; Resources: 11.0 CPU, 1.8KB object store:  38%|███▊      | 75.0/200 [00:52<0
  Running Dataset: dataset_15_0. Active & requested resources: 67/70 CPU, 1.2GB/39.9GB object store:  38%|███▊      | 75.0/200 [00:53<00:33, 3.78 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 67/70 CPU, 1.2GB/39.9GB object store:  38%|███▊      | 75.0/200 [00:53<00:33, 3.78 row/s]
  
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.6MB
   object store; [all objects local]:  43%|████▎     | 14.6M/34.0M [00:53<00:46, 416k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.6MB
   object store; [all objects local]:  44%|████▎     | 14.6M/33.5M [00:53<00:45, 416k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.6MB
   object store; [all objects local]:  45%|████▍     | 14.9M/33.5M [00:53<00:48, 385k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.6MB
  - ReadParquet: Tasks: 20 [backpressured:tasks]; Actors: 0; Queued blocks: 91; Resources: 20.0 CPU, 1.1GB object store:  44%|
  ████▍     | 14.8M/33.6M [00:53<00:47, 398k row/s]
  - ReadParquet: Tasks: 20 [backpressured:tasks]; Actors: 0; Queued blocks: 91; Resources: 20.0 CPU, 1.1GB object store:  44%|
  ████▍     | 14.8M/33.5M [00:53<00:46, 398k row/s]
  - ReadParquet: Tasks: 20 [backpressured:tasks]; Actors: 0; Queued blocks: 91; Resources: 20.0 CPU, 1.1GB object store:  45%|
  ████▍     | 14.9M/33.5M [00:53<00:55, 333k row/s]
  - ReadParquet: Tasks: 20 [backpressured:tasks]; Actors: 0; Queued blocks: 91; Resources: 20.0 CPU, 1.1GB object store:  45%|
  ████▍     | 14.9M/33.5M [00:53<00:55, 333k row/s]
  
  
  - Write: Tasks: 14; Actors: 0; Queued blocks: 0; Resources: 14.0 CPU, 2.3KB object store:  38%|███▊      | 75.0/200 [00:53<0
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 1.1GB/39.9GB object store:  38%|███▊      | 75.0/200 [00:54<00:33, 3.78 row/s]: 0; Queued blocks: 0; Resources: 14.0 CPU, 2.3KB object store:  38%|███▊      | 75.0/200 [00:53<0
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 1.1GB/39.9GB object store:  38%|███▊      | 75.0/200 [00:54<00:33, 3.78 row/s]
  
  - ReadParquet: Tasks: 18 [backpressured:tasks]; Actors: 0; Queued blocks: 90; Resources: 18.0 CPU, 1.0GB object store:  45%|
  ████▍     | 14.9M/33.5M [00:54<00:55, 333k row/s]
  - ReadParquet: Tasks: 18 [backpressured:tasks]; Actors: 0; Queued blocks: 90; Resources: 18.0 CPU, 1.0GB object store:  45%|
  ████▌     | 14.9M/33.0M [00:54<00:54, 333k row/s]
  - ReadParquet: Tasks: 18 [backpressured:tasks]; Actors: 0; Queued blocks: 90; Resources: 18.0 CPU, 1.0GB object store:  46%|
  ████▌     | 15.2M/33.0M [00:54<00:55, 322k row/s]
  - ReadParquet: Tasks: 18 [backpressured:tasks]; Actors: 0; Queued blocks: 90; Resources: 18.0 CPU, 1.0GB object store:  46%|
  ████▌     | 15.2M/33.0M [00:54<00:55, 322k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.4MB
   object store; [all objects local]:  45%|████▍     | 14.9M/33.5M [00:54<00:48, 385k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.4MB
   object store; [all objects local]:  45%|████▍     | 14.9M/33.2M [00:54<00:47, 385k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.4MB
   object store; [all objects local]:  46%|████▌     | 15.1M/33.2M [00:54<00:54, 334k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.4MB
   object store; [all objects local]:  46%|████▌     | 15.1M/33.2M [00:54<00:54, 334k row/s]
  
  - Write: Tasks: 15; Actors: 0; Queued blocks: 0; Resources: 15.0 CPU, 2.5KB object store:  38%|███▊      | 75.0/200 [00:54<0
  0:31, 3.96 row/s]
  - Write: Tasks: 15; Actors: 0; Queued blocks: 0; Resources: 15.0 CPU, 2.5KB object store:  38%|███▊      | 76.0/200 [00:54<0
  Running Dataset: dataset_15_0. Active & requested resources: 67/70 CPU, 1.0GB/39.9GB object store:  38%|███▊      | 75.0/200 [00:55<00:33, 3.78 row/s]: 0; Queued blocks: 0; Resources: 15.0 CPU, 2.5KB object store:  38%|███▊      | 76.0/200 [00:54<0
  Running Dataset: dataset_15_0. Active & requested resources: 67/70 CPU, 1.0GB/39.9GB object store:  38%|███▊      | 76.0/200 [00:55<00:52, 2.38 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 67/70 CPU, 1.0GB/39.9GB object store:  38%|███▊      | 76.0/200 [00:55<00:52, 2.38 row/s]
  
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 85; Resources: 21.0 CPU, 1.2GB object store:  46%|
  ████▌     | 15.2M/33.0M [00:55<00:55, 322k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 85; Resources: 21.0 CPU, 1.2GB object store:  46%|
  ████▋     | 15.2M/32.8M [00:55<00:54, 322k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 85; Resources: 21.0 CPU, 1.2GB object store:  47%|
  ████▋     | 15.4M/32.8M [00:55<01:00, 288k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 85; Resources: 21.0 CPU, 1.2GB object store:  47%|
  ████▋     | 15.4M/32.8M [00:55<01:00, 288k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 4.7MB
   object store; [all objects local]:  46%|████▌     | 15.1M/33.2M [00:55<00:54, 334k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 4.7MB
   object store; [all objects local]:  46%|████▌     | 15.1M/33.0M [00:55<00:53, 334k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 4.7MB
   object store; [all objects local]:  46%|████▌     | 15.2M/33.0M [00:55<01:06, 269k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 4.7MB
   object store; [all objects local]:  46%|████▌     | 15.2M/33.0M [00:55<01:06, 269k row/s]
  
  - Write: Tasks: 9; Actors: 0; Queued blocks: 0; Resources: 9.0 CPU, 1.5KB object store:  38%|███▊      | 76.0/200 [00:55<00:
  50, 2.44 row/s]
  - Write: Tasks: 9; Actors: 0; Queued blocks: 0; Resources: 9.0 CPU, 1.5KB object store:  42%|████▏     | 83.0/200 [00:55<00:
  Running Dataset: dataset_15_0. Active & requested resources: 65/70 CPU, 1.2GB/39.9GB object store:  38%|███▊      | 76.0/200 [00:56<00:52, 2.38 row/s] 0; Queued blocks: 0; Resources: 9.0 CPU, 1.5KB object store:  42%|████▏     | 83.0/200 [00:55<00:
  Running Dataset: dataset_15_0. Active & requested resources: 65/70 CPU, 1.2GB/39.9GB object store:  42%|████▏     | 83.0/200 [00:56<00:34, 3.37 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 65/70 CPU, 1.2GB/39.9GB object store:  42%|████▏     | 83.0/200 [00:56<00:34, 3.37 row/s]
  
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 82; Resources: 21.0 CPU, 1.1GB object store:  47%|
  ████▋     | 15.4M/32.8M [00:56<01:00, 288k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 82; Resources: 21.0 CPU, 1.1GB object store:  48%|
  ████▊     | 15.4M/32.2M [00:56<00:58, 288k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 82; Resources: 21.0 CPU, 1.1GB object store:  49%|
  ████▉     | 15.8M/32.2M [00:56<00:52, 311k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 82; Resources: 21.0 CPU, 1.1GB object store:  49%|
  ████▉     | 15.8M/32.2M [00:56<00:52, 311k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.4MB
   object store; [all objects local]:  46%|████▌     | 15.2M/33.0M [00:56<01:06, 269k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.4MB
   object store; [all objects local]:  47%|████▋     | 15.2M/32.5M [00:56<01:04, 269k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.4MB
   object store; [all objects local]:  48%|████▊     | 15.6M/32.5M [00:56<00:56, 298k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.4MB
   object store; [all objects local]:  48%|████▊     | 15.6M/32.5M [00:56<00:56, 298k row/s]
  
  - Write: Tasks: 11; Actors: 0; Queued blocks: 0; Resources: 11.0 CPU, 1.8KB object store:  42%|████▏     | 83.0/200 [00:56<0
  0:34, 3.44 row/s]
  - Write: Tasks: 11; Actors: 0; Queued blocks: 0; Resources: 11.0 CPU, 1.8KB object store:  42%|████▎     | 85.0/200 [00:56<0
  Running Dataset: dataset_15_0. Active & requested resources: 67/70 CPU, 1.1GB/39.9GB object store:  42%|████▏     | 83.0/200 [00:57<00:34, 3.37 row/s]: 0; Queued blocks: 0; Resources: 11.0 CPU, 1.8KB object store:  42%|████▎     | 85.0/200 [00:56<0
  Running Dataset: dataset_15_0. Active & requested resources: 67/70 CPU, 1.1GB/39.9GB object store:  42%|████▎     | 85.0/200 [00:57<00:38, 2.99 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 67/70 CPU, 1.1GB/39.9GB object store:  42%|████▎     | 85.0/200 [00:57<00:38, 2.99 row/s]
  
  - ReadParquet: Tasks: 20 [backpressured:tasks]; Actors: 0; Queued blocks: 73; Resources: 20.0 CPU, 1.1GB object store:  49%|
  ████▉     | 15.8M/32.2M [00:57<00:52, 311k row/s]
  - ReadParquet: Tasks: 20 [backpressured:tasks]; Actors: 0; Queued blocks: 73; Resources: 20.0 CPU, 1.1GB object store:  51%|
  █████     | 15.8M/31.2M [00:57<00:49, 311k row/s]
  - ReadParquet: Tasks: 20 [backpressured:tasks]; Actors: 0; Queued blocks: 73; Resources: 20.0 CPU, 1.1GB object store:  54%|
  █████▎    | 16.7M/31.2M [00:57<00:31, 466k row/s]
  - ReadParquet: Tasks: 20 [backpressured:tasks]; Actors: 0; Queued blocks: 73; Resources: 20.0 CPU, 1.1GB object store:  54%|
  █████▎    | 16.7M/31.2M [00:57<00:31, 466k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 4; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 7.8MB
   object store; [all objects local]:  48%|████▊     | 15.6M/32.5M [00:57<00:56, 298k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 4; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 7.8MB
   object store; [all objects local]:  50%|████▉     | 15.6M/31.3M [00:57<00:52, 298k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 4; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 7.8MB
   object store; [all objects local]:  52%|█████▏    | 16.4M/31.3M [00:57<00:35, 423k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 4; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 7.8MB
   object store; [all objects local]:  52%|█████▏    | 16.4M/31.3M [00:57<00:35, 423k row/s]
  
  - Write: Tasks: 14; Actors: 0; Queued blocks: 0; Resources: 14.0 CPU, 2.3KB object store:  42%|████▎     | 85.0/200 [00:57<0
  0:37, 3.03 row/s]
  - Write: Tasks: 14; Actors: 0; Queued blocks: 0; Resources: 14.0 CPU, 2.3KB object store:  45%|████▌     | 90.0/200 [00:57<0
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 1.1GB/39.9GB object store:  42%|████▎     | 85.0/200 [00:58<00:38, 2.99 row/s]: 0; Queued blocks: 0; Resources: 14.0 CPU, 2.3KB object store:  45%|████▌     | 90.0/200 [00:57<0
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 1.1GB/39.9GB object store:  45%|████▌     | 90.0/200 [00:58<00:31, 3.49 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 1.1GB/39.9GB object store:  45%|████▌     | 90.0/200 [00:58<00:31, 3.49 row/s]
  
  - ReadParquet: Tasks: 18 [backpressured:tasks]; Actors: 0; Queued blocks: 73; Resources: 18.0 CPU, 929.9MB object store:  54
  %|█████▎    | 16.7M/31.2M [00:58<00:31, 466k row/s]
  - ReadParquet: Tasks: 18 [backpressured:tasks]; Actors: 0; Queued blocks: 73; Resources: 18.0 CPU, 929.9MB object store:  54
  %|█████▍    | 16.7M/31.0M [00:58<00:30, 466k row/s]
  - ReadParquet: Tasks: 18 [backpressured:tasks]; Actors: 0; Queued blocks: 73; Resources: 18.0 CPU, 929.9MB object store:  55
  %|█████▍    | 16.9M/31.0M [00:58<00:36, 387k row/s]
  - ReadParquet: Tasks: 18 [backpressured:tasks]; Actors: 0; Queued blocks: 73; Resources: 18.0 CPU, 929.9MB object store:  55
  %|█████▍    | 16.9M/31.0M [00:58<00:36, 387k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.5MB
   object store; [all objects local]:  52%|█████▏    | 16.4M/31.3M [00:58<00:35, 423k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.5MB
   object store; [all objects local]:  53%|█████▎    | 16.4M/31.0M [00:58<00:34, 423k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.5MB
   object store; [all objects local]:  55%|█████▍    | 16.9M/31.0M [00:58<00:32, 438k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.5MB
   object store; [all objects local]:  55%|█████▍    | 16.9M/31.0M [00:58<00:32, 438k row/s]
  
  - Write: Tasks: 17 [backpressured:tasks]; Actors: 0; Queued blocks: 0; Resources: 17.0 CPU, 2.8KB object store:  45%|████▌
     | 90.0/200 [00:58<00:31, 3.44 row/s]
  - Write: Tasks: 17 [backpressured:tasks]; Actors: 0; Queued blocks: 0; Resources: 17.0 CPU, 2.8KB object store:  46%|████▌
  Running Dataset: dataset_15_0. Active & requested resources: 70/70 CPU, 936.4MB/39.9GB object store:  45%|████▌     | 90.0/200 [01:00<00:31, 3.49 row/s]sured:tasks]; Actors: 0; Queued blocks: 0; Resources: 17.0 CPU, 2.8KB object store:  46%|████▌
  Running Dataset: dataset_15_0. Active & requested resources: 70/70 CPU, 936.4MB/39.9GB object store:  46%|████▌     | 92.0/200 [01:00<00:35, 3.06 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 70/70 CPU, 936.4MB/39.9GB object store:  46%|████▌     | 92.0/200 [01:00<00:35, 3.06 row/s]
  
  - ReadParquet: Tasks: 19 [backpressured:tasks]; Actors: 0; Queued blocks: 71; Resources: 19.0 CPU, 978.4MB object store:  55
  %|█████▍    | 16.9M/31.0M [00:59<00:36, 387k row/s]
  - ReadParquet: Tasks: 19 [backpressured:tasks]; Actors: 0; Queued blocks: 71; Resources: 19.0 CPU, 978.4MB object store:  55
  %|█████▍    | 16.9M/30.9M [00:59<00:36, 387k row/s]
  - ReadParquet: Tasks: 19 [backpressured:tasks]; Actors: 0; Queued blocks: 71; Resources: 19.0 CPU, 978.4MB object store:  55
  %|█████▍    | 17.0M/30.9M [00:59<00:45, 303k row/s]
  - ReadParquet: Tasks: 19 [backpressured:tasks]; Actors: 0; Queued blocks: 71; Resources: 19.0 CPU, 978.4MB object store:  55
  %|█████▍    | 17.0M/30.9M [00:59<00:45, 303k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.7MB
   object store; [all objects local]:  55%|█████▍    | 16.9M/31.0M [00:59<00:32, 438k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.7MB
   object store; [all objects local]:  55%|█████▍    | 16.9M/30.9M [00:59<00:31, 438k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.7MB
   object store; [all objects local]:  55%|█████▍    | 17.0M/30.9M [00:59<00:41, 339k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.7MB
   object store; [all objects local]:  55%|█████▍    | 17.0M/30.9M [00:59<00:41, 339k row/s]
  
  - Write: Tasks: 15; Actors: 0; Queued blocks: 0; Resources: 15.0 CPU, 2.5KB object store:  46%|████▌     | 92.0/200 [00:59<0
  0:35, 3.02 row/s]
  - Write: Tasks: 15; Actors: 0; Queued blocks: 0; Resources: 15.0 CPU, 2.5KB object store:  48%|████▊     | 95.0/200 [00:59<0
  Running Dataset: dataset_15_0. Active & requested resources: 69/70 CPU, 984.1MB/39.9GB object store:  46%|████▌     | 92.0/200 [01:01<00:35, 3.06 row/s]0; Queued blocks: 0; Resources: 15.0 CPU, 2.5KB object store:  48%|████▊     | 95.0/200 [00:59<0
  Running Dataset: dataset_15_0. Active & requested resources: 69/70 CPU, 984.1MB/39.9GB object store:  48%|████▊     | 95.0/200 [01:01<00:34, 3.00 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 69/70 CPU, 984.1MB/39.9GB object store:  48%|████▊     | 95.0/200 [01:01<00:34, 3.00 row/s]
  
  - ReadParquet: Tasks: 20 [backpressured:tasks]; Actors: 0; Queued blocks: 67; Resources: 20.0 CPU, 1.1GB object store:  55%|
  █████▍    | 17.0M/30.9M [01:00<00:45, 303k row/s]
  - ReadParquet: Tasks: 20 [backpressured:tasks]; Actors: 0; Queued blocks: 67; Resources: 20.0 CPU, 1.1GB object store:  56%|
  █████▌    | 17.0M/30.5M [01:00<00:44, 303k row/s]
  - ReadParquet: Tasks: 20 [backpressured:tasks]; Actors: 0; Queued blocks: 67; Resources: 20.0 CPU, 1.1GB object store:  57%|
  █████▋    | 17.4M/30.5M [01:00<00:40, 325k row/s]
  - ReadParquet: Tasks: 20 [backpressured:tasks]; Actors: 0; Queued blocks: 67; Resources: 20.0 CPU, 1.1GB object store:  57%|
  █████▋    | 17.4M/30.5M [01:00<00:40, 325k row/s]
  
  
  - Write: Tasks: 12; Actors: 0; Queued blocks: 0; Resources: 12.0 CPU, 2.3KB object store:  48%|████▊     | 95.0/200 [01:00<0
  0:35, 2.98 row/s]
  - Write: Tasks: 12; Actors: 0; Queued blocks: 0; Resources: 12.0 CPU, 2.3KB object store:  50%|█████     | 101/200 [01:00<00
  Running Dataset: dataset_15_0. Active & requested resources: 67/70 CPU, 1.1GB/39.9GB object store:  48%|████▊     | 95.0/200 [01:02<00:34, 3.00 row/s]  0; Queued blocks: 0; Resources: 12.0 CPU, 2.3KB object store:  50%|█████     | 101/200 [01:00<00
  Running Dataset: dataset_15_0. Active & requested resources: 67/70 CPU, 1.1GB/39.9GB object store:  50%|█████     | 100/200 [01:02<00:28, 3.49 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 67/70 CPU, 1.1GB/39.9GB object store:  50%|█████     | 100/200 [01:02<00:28, 3.49 row/s]
  
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.4MB
   object store; [all objects local]:  55%|█████▍    | 17.0M/30.9M [01:00<00:41, 339k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.4MB
   object store; [all objects local]:  55%|█████▌    | 17.0M/30.7M [01:00<00:40, 339k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.4MB
   object store; [all objects local]:  56%|█████▌    | 17.2M/30.7M [01:00<00:46, 294k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.4MB
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 62; Resources: 21.0 CPU, 1.1GB object store:  57%|
  █████▋    | 17.4M/30.5M [01:01<00:40, 325k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 62; Resources: 21.0 CPU, 1.1GB object store:  58%|
  █████▊    | 17.4M/30.3M [01:01<00:39, 325k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 62; Resources: 21.0 CPU, 1.1GB object store:  59%|
  █████▊    | 17.7M/30.3M [01:01<00:40, 311k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 62; Resources: 21.0 CPU, 1.1GB object store:  59%|
  █████▊    | 17.7M/30.3M [01:01<00:40, 311k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 3.6MB
   object store; [all objects local]:  56%|█████▌    | 17.2M/30.7M [01:02<00:46, 294k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 3.6MB
   object store; [all objects local]:  57%|█████▋    | 17.2M/30.3M [01:02<00:44, 294k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 3.6MB
   object store; [all objects local]:  58%|█████▊    | 17.6M/30.3M [01:02<00:40, 318k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 3.6MB
   object store; [all objects local]:  58%|█████▊    | 17.6M/30.3M [01:02<00:40, 318k row/s]
  
  - Write: Tasks: 8; Actors: 0; Queued blocks: 0; Resources: 8.0 CPU, 1.3KB object store:  50%|█████     | 101/200 [01:02<00:2
  6, 3.74 row/s]
  - Write: Tasks: 8; Actors: 0; Queued blocks: 0; Resources: 8.0 CPU, 1.3KB object store:  54%|█████▍    | 108/200 [01:02<00:2
  Running Dataset: dataset_15_0. Active & requested resources: 64/70 CPU, 1.1GB/39.9GB object store:  50%|█████     | 100/200 [01:03<00:28, 3.49 row/s]: 0; Queued blocks: 0; Resources: 8.0 CPU, 1.3KB object store:  54%|█████▍    | 108/200 [01:02<00:2
  Running Dataset: dataset_15_0. Active & requested resources: 64/70 CPU, 1.1GB/39.9GB object store:  54%|█████▍    | 108/200 [01:03<00:19, 4.67 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 64/70 CPU, 1.1GB/39.9GB object store:  54%|█████▍    | 108/200 [01:03<00:19, 4.67 row/s]
  
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 57; Resources: 21.0 CPU, 1.1GB object store:  59%|
  █████▊    | 17.7M/30.3M [01:02<00:40, 311k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 57; Resources: 21.0 CPU, 1.1GB object store:  59%|
  █████▉    | 17.7M/29.8M [01:02<00:38, 311k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 57; Resources: 21.0 CPU, 1.1GB object store:  61%|
  ██████▏   | 18.3M/29.8M [01:02<00:29, 386k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 57; Resources: 21.0 CPU, 1.1GB object store:  61%|
  Running Dataset: dataset_15_0. Active & requested resources: 67/70 CPU, 1.1GB/39.9GB object store:  54%|█████▍    | 108/200 [01:04<00:19, 4.67 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 67/70 CPU, 1.1GB/39.9GB object store:  55%|█████▌    | 110/200 [01:04<00:23, 3.83 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 67/70 CPU, 1.1GB/39.9GB object store:  55%|█████▌    | 110/200 [01:04<00:23, 3.83 row/s]
  
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.3MB
   object store; [all objects local]:  58%|█████▊    | 17.6M/30.3M [01:03<00:40, 318k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.3MB
   object store; [all objects local]:  59%|█████▉    | 17.6M/29.9M [01:03<00:38, 318k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.3MB
   object store; [all objects local]:  61%|██████    | 18.1M/29.9M [01:03<00:32, 362k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.3MB
   object store; [all objects local]:  61%|██████    | 18.1M/29.9M [01:03<00:32, 362k row/s]
  
  - Write: Tasks: 11; Actors: 0; Queued blocks: 0; Resources: 11.0 CPU, 1.8KB object store:  54%|█████▍    | 108/200 [01:03<00
  :20, 4.56 row/s]
  - Write: Tasks: 11; Actors: 0; Queued blocks: 0; Resources: 11.0 CPU, 1.8KB object store:  55%|█████▌    | 110/200 [01:03<00
  :23, 3.76 row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 52; Resources: 21.0 CPU, 1.0GB object store:  61%|
  ██████▏   | 18.3M/29.8M [01:04<00:29, 386k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 52; Resources: 21.0 CPU, 1.0GB object store:  62%|
  ██████▏   | 18.3M/29.4M [01:04<00:28, 386k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 52; Resources: 21.0 CPU, 1.0GB object store:  63%|
  ██████▎   | 18.7M/29.4M [01:04<00:28, 382k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 52; Resources: 21.0 CPU, 1.0GB object store:  63%|
  ██████▎   | 18.7M/29.4M [01:04<00:28, 382k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.1MB
   object store; [all objects local]:  61%|██████    | 18.1M/29.9M [01:04<00:32, 362k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.1MB
   object store; [all objects local]:  61%|██████▏   | 18.1M/29.4M [01:04<00:31, 362k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.1MB
   object store; [all objects local]:  63%|██████▎   | 18.7M/29.4M [01:04<00:25, 422k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.1MB
   object store; [all objects local]:  63%|██████▎   | 18.7M/29.4M [01:04<00:25, 422k row/s]
  
  - Write: Tasks: 14 [backpressured:tasks]; Actors: 0; Queued blocks: 2; Resources: 14.0 CPU, 2.3KB object store:  55%|█████▌
     | 110/200 [01:04<00:23, 3.76 row/s]
  - Write: Tasks: 14 [backpressured:tasks]; Actors: 0; Queued blocks: 2; Resources: 14.0 CPU, 2.3KB object store:  56%|█████▌
  Running Dataset: dataset_15_0. Active & requested resources: 70/70 CPU, 1.0GB/39.9GB object store:  55%|█████▌    | 110/200 [01:05<00:23, 3.83 row/s]ressured:tasks]; Actors: 0; Queued blocks: 2; Resources: 14.0 CPU, 2.3KB object store:  56%|█████▌
  Running Dataset: dataset_15_0. Active & requested resources: 70/70 CPU, 1.0GB/39.9GB object store:  56%|█████▌    | 111/200 [01:05<00:29, 2.98 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 70/70 CPU, 1.0GB/39.9GB object store:  56%|█████▌    | 111/200 [01:05<00:29, 2.98 row/s]
  
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 51; Resources: 21.0 CPU, 1.0GB object store:  63%|
  ██████▎   | 18.7M/29.4M [01:05<00:28, 382k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 51; Resources: 21.0 CPU, 1.0GB object store:  64%|
  ██████▎   | 18.7M/29.4M [01:05<00:27, 382k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 51; Resources: 21.0 CPU, 1.0GB object store:  64%|
  ██████▍   | 18.8M/29.4M [01:05<00:35, 297k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 51; Resources: 21.0 CPU, 1.0GB object store:  64%|
  Running Dataset: dataset_15_0. Active & requested resources: 66/70 CPU, 1.0GB/39.9GB object store:  56%|█████▌    | 111/200 [01:06<00:29, 2.98 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 66/70 CPU, 1.0GB/39.9GB object store:  58%|█████▊    | 116/200 [01:06<00:23, 3.50 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 66/70 CPU, 1.0GB/39.9GB object store:  58%|█████▊    | 116/200 [01:06<00:23, 3.50 row/s]
  
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.3MB
   object store; [all objects local]:  63%|██████▎   | 18.7M/29.4M [01:05<00:25, 422k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.3MB
   object store; [all objects local]:  63%|██████▎   | 18.7M/29.4M [01:05<00:25, 422k row/s]
  
  - Write: Tasks: 11; Actors: 0; Queued blocks: 0; Resources: 11.0 CPU, 1.8KB object store:  56%|█████▌    | 111/200 [01:05<00
  :30, 2.93 row/s]
  - Write: Tasks: 11; Actors: 0; Queued blocks: 0; Resources: 11.0 CPU, 1.8KB object store:  58%|█████▊    | 116/200 [01:05<00
  :24, 3.46 row/s]
  - ReadParquet: Tasks: 20 [backpressured:tasks]; Actors: 0; Queued blocks: 47; Resources: 20.0 CPU, 1.0GB object store:  64%|
  ██████▍   | 18.8M/29.4M [01:06<00:35, 297k row/s]
  - ReadParquet: Tasks: 20 [backpressured:tasks]; Actors: 0; Queued blocks: 47; Resources: 20.0 CPU, 1.0GB object store:  65%|
  ██████▍   | 18.8M/29.0M [01:06<00:34, 297k row/s]
  - ReadParquet: Tasks: 20 [backpressured:tasks]; Actors: 0; Queued blocks: 47; Resources: 20.0 CPU, 1.0GB object store:  67%|
  ██████▋   | 19.3M/29.0M [01:06<00:27, 349k row/s]
  - ReadParquet: Tasks: 20 [backpressured:tasks]; Actors: 0; Queued blocks: 47; Resources: 20.0 CPU, 1.0GB object store:  67%|
  ██████▋   | 19.3M/29.0M [01:06<00:27, 349k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 3; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.6MB
   object store; [all objects local]:  63%|██████▎   | 18.7M/29.4M [01:06<00:25, 422k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 3; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.6MB
   object store; [all objects local]:  64%|██████▍   | 18.7M/29.2M [01:06<00:24, 422k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 3; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.6MB
   object store; [all objects local]:  66%|██████▌   | 19.1M/29.2M [01:06<00:31, 315k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 3; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.6MB
   object store; [all objects local]:  66%|██████▌   | 19.1M/29.2M [01:06<00:31, 315k row/s]
  
  - Write: Tasks: 14; Actors: 0; Queued blocks: 0; Resources: 14.0 CPU, 2.3KB object store:  58%|█████▊    | 116/200 [01:06<00
  :24, 3.46 row/s]
  - Write: Tasks: 14; Actors: 0; Queued blocks: 0; Resources: 14.0 CPU, 2.3KB object store:  58%|█████▊    | 117/200 [01:06<00
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 1.0GB/39.9GB object store:  58%|█████▊    | 116/200 [01:07<00:23, 3.50 row/s]s: 0; Queued blocks: 0; Resources: 14.0 CPU, 2.3KB object store:  58%|█████▊    | 117/200 [01:06<00
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 1.0GB/39.9GB object store:  58%|█████▊    | 117/200 [01:07<00:30, 2.73 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 1.0GB/39.9GB object store:  58%|█████▊    | 117/200 [01:07<00:30, 2.73 row/s]
  
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 42; Resources: 21.0 CPU, 1.0GB object store:  67%|
  ██████▋   | 19.3M/29.0M [01:07<00:27, 349k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 42; Resources: 21.0 CPU, 1.0GB object store:  67%|
  ██████▋   | 19.3M/28.8M [01:07<00:27, 349k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 42; Resources: 21.0 CPU, 1.0GB object store:  69%|
  ██████▊   | 19.7M/28.8M [01:07<00:25, 356k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 42; Resources: 21.0 CPU, 1.0GB object store:  69%|
  ██████▊   | 19.7M/28.8M [01:07<00:25, 356k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.7MB
   object store; [all objects local]:  66%|██████▌   | 19.1M/29.2M [01:07<00:31, 315k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.7MB
   object store; [all objects local]:  66%|██████▋   | 19.1M/28.8M [01:07<00:30, 315k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.7MB
   object store; [all objects local]:  68%|██████▊   | 19.6M/28.8M [01:07<00:26, 353k row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 1.0GB/39.9GB object store:  62%|██████▏   | 123/200 [01:08<00:21, 3.59 row/s]ts local]:  68%|██████▊   | 19.6M/28.8M [01:07<00:26, 353k row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 1.0GB/39.9GB object store:  62%|██████▏   | 123/200 [01:08<00:21, 3.59 row/s]
  
  
  
  - Write: Tasks: 13; Actors: 0; Queued blocks: 0; Resources: 13.0 CPU, 2.1KB object store:  58%|█████▊    | 117/200 [01:07<00
  :30, 2.71 row/s]
  - Write: Tasks: 13; Actors: 0; Queued blocks: 0; Resources: 13.0 CPU, 2.1KB object store:  62%|██████▏   | 123/200 [01:07<00
  :21, 3.57 row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 38; Resources: 21.0 CPU, 1.0GB object store:  69%|
  ██████▊   | 19.7M/28.8M [01:08<00:25, 356k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 38; Resources: 21.0 CPU, 1.0GB object store:  69%|
  ██████▉   | 19.7M/28.5M [01:08<00:24, 356k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 38; Resources: 21.0 CPU, 1.0GB object store:  71%|
  ███████   | 20.2M/28.5M [01:08<00:21, 389k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 38; Resources: 21.0 CPU, 1.0GB object store:  71%|
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 1.1GB/39.9GB object store:  62%|██████▏   | 123/200 [01:09<00:21, 3.59 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 1.1GB/39.9GB object store:  64%|██████▎   | 127/200 [01:09<00:20, 3.63 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 1.1GB/39.9GB object store:  64%|██████▎   | 127/200 [01:09<00:20, 3.63 row/s]
  
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 3; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.2MB
   object store; [all objects local]:  68%|██████▊   | 19.6M/28.8M [01:08<00:26, 353k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 3; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.2MB
   object store; [all objects local]:  68%|██████▊   | 19.6M/28.6M [01:08<00:25, 353k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 3; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.2MB
   object store; [all objects local]:  69%|██████▉   | 19.9M/28.6M [01:08<00:26, 333k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 3; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.2MB
   object store; [all objects local]:  69%|██████▉   | 19.9M/28.6M [01:08<00:26, 333k row/s]
  
  - Write: Tasks: 12; Actors: 0; Queued blocks: 0; Resources: 12.0 CPU, 2.0KB object store:  62%|██████▏   | 123/200 [01:08<00
  :21, 3.57 row/s]
  - Write: Tasks: 12; Actors: 0; Queued blocks: 0; Resources: 12.0 CPU, 2.0KB object store:  64%|██████▎   | 127/200 [01:08<00
  :20, 3.62 row/s]
  - ReadParquet: Tasks: 18 [backpressured:tasks]; Actors: 0; Queued blocks: 35; Resources: 18.0 CPU, 911.2MB object store:  71
  %|███████   | 20.2M/28.5M [01:09<00:21, 389k row/s]
  - ReadParquet: Tasks: 18 [backpressured:tasks]; Actors: 0; Queued blocks: 35; Resources: 18.0 CPU, 911.2MB object store:  72
  %|███████▏  | 20.2M/28.1M [01:09<00:20, 389k row/s]
  - ReadParquet: Tasks: 18 [backpressured:tasks]; Actors: 0; Queued blocks: 35; Resources: 18.0 CPU, 911.2MB object store:  74
  %|███████▍  | 20.8M/28.1M [01:09<00:16, 440k row/s]
  - ReadParquet: Tasks: 18 [backpressured:tasks]; Actors: 0; Queued blocks: 35; Resources: 18.0 CPU, 911.2MB object store:  74
  Running Dataset: dataset_15_0. Active & requested resources: 70/70 CPU, 837.2MB/39.9GB object store:  64%|██████▎   | 127/200 [01:10<00:20, 3.63 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 70/70 CPU, 837.2MB/39.9GB object store:  64%|██████▍   | 128/200 [01:10<00:25, 2.82 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 70/70 CPU, 837.2MB/39.9GB object store:  64%|██████▍   | 128/200 [01:10<00:25, 2.82 row/s]
  
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 7.8MB
   object store; [all objects local]:  69%|██████▉   | 19.9M/28.6M [01:09<00:26, 333k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 7.8MB
   object store; [all objects local]:  71%|███████   | 19.9M/28.2M [01:09<00:24, 333k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 7.8MB
   object store; [all objects local]:  74%|███████▎  | 20.7M/28.2M [01:09<00:16, 445k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 7.8MB
   object store; [all objects local]:  74%|███████▎  | 20.7M/28.2M [01:09<00:16, 445k row/s]
  
  - Write: Tasks: 18 [backpressured:tasks]; Actors: 0; Queued blocks: 1; Resources: 18.0 CPU, 3.0KB object store:  64%|██████▎
     | 127/200 [01:09<00:20, 3.62 row/s]
  - Write: Tasks: 18 [backpressured:tasks]; Actors: 0; Queued blocks: 1; Resources: 18.0 CPU, 3.0KB object store:  64%|██████▍
     | 128/200 [01:09<00:25, 2.81 row/s]
  - ReadParquet: Tasks: 18 [backpressured:tasks]; Actors: 0; Queued blocks: 34; Resources: 18.0 CPU, 842.9MB object store:  74
  %|███████▍  | 20.8M/28.1M [01:10<00:16, 440k row/s]
  - ReadParquet: Tasks: 18 [backpressured:tasks]; Actors: 0; Queued blocks: 34; Resources: 18.0 CPU, 842.9MB object store:  74
  %|███████▍  | 20.8M/28.1M [01:10<00:16, 440k row/s]
  
  
  - Write: Tasks: 15; Actors: 0; Queued blocks: 0; Resources: 15.0 CPU, 2.6KB object store:  64%|██████▍   | 128/200 [01:10<00
  :25, 2.81 row/s]
  - Write: Tasks: 15; Actors: 0; Queued blocks: 0; Resources: 15.0 CPU, 2.6KB object store:  67%|██████▋   | 134/200 [01:10<00
  Running Dataset: dataset_15_0. Active & requested resources: 66/70 CPU, 833.2MB/39.9GB object store:  64%|██████▍   | 128/200 [01:11<00:25, 2.82 row/s] 0; Queued blocks: 0; Resources: 15.0 CPU, 2.6KB object store:  67%|██████▋   | 134/200 [01:10<00
  Running Dataset: dataset_15_0. Active & requested resources: 66/70 CPU, 833.2MB/39.9GB object store:  66%|██████▋   | 133/200 [01:11<00:19, 3.39 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 66/70 CPU, 833.2MB/39.9GB object store:  66%|██████▋   | 133/200 [01:11<00:19, 3.39 row/s]
  
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.9MB
   object store; [all objects local]:  74%|███████▎  | 20.7M/28.2M [01:10<00:16, 445k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.9MB
   object store; [all objects local]:  74%|███████▎  | 20.7M/28.1M [01:10<00:16, 445k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.9MB
   object store; [all objects local]:  74%|███████▍  | 20.8M/28.1M [01:10<00:20, 348k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.9MB
  - ReadParquet: Tasks: 20 [backpressured:tasks]; Actors: 0; Queued blocks: 28; Resources: 20.0 CPU, 962.7MB object store:  74
  %|███████▍  | 20.8M/28.1M [01:11<00:16, 440k row/s]
  - ReadParquet: Tasks: 20 [backpressured:tasks]; Actors: 0; Queued blocks: 28; Resources: 20.0 CPU, 962.7MB object store:  75
  %|███████▍  | 20.8M/27.9M [01:11<00:16, 440k row/s]
  - ReadParquet: Tasks: 20 [backpressured:tasks]; Actors: 0; Queued blocks: 28; Resources: 20.0 CPU, 962.7MB object store:  76
  %|███████▌  | 21.2M/27.9M [01:11<00:20, 324k row/s]
  - ReadParquet: Tasks: 20 [backpressured:tasks]; Actors: 0; Queued blocks: 28; Resources: 20.0 CPU, 962.7MB object store:  76
  %|███████▌  | 21.2M/27.9M [01:11<00:20, 324k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.9MB
   object store; [all objects local]:  75%|███████▍  | 20.8M/27.9M [01:11<00:20, 348k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.9MB
   object store; [all objects local]:  76%|███████▌  | 21.2M/27.9M [01:11<00:18, 356k row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 69/70 CPU, 935.1MB/39.9GB object store:  66%|██████▋   | 133/200 [01:12<00:19, 3.39 row/s] local]:  76%|███████▌  | 21.2M/27.9M [01:11<00:18, 356k row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 69/70 CPU, 935.1MB/39.9GB object store:  68%|██████▊   | 137/200 [01:12<00:18, 3.50 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 69/70 CPU, 935.1MB/39.9GB object store:  68%|██████▊   | 137/200 [01:12<00:18, 3.50 row/s]
  
  
  
  - Write: Tasks: 15 [backpressured:tasks]; Actors: 0; Queued blocks: 0; Resources: 15.0 CPU, 2.5KB object store:  67%|██████▋
     | 134/200 [01:11<00:17, 3.67 row/s]
  - Write: Tasks: 15 [backpressured:tasks]; Actors: 0; Queued blocks: 0; Resources: 15.0 CPU, 2.5KB object store:  68%|██████▊
     | 137/200 [01:11<00:18, 3.41 row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 26; Resources: 21.0 CPU, 974.1MB object store:  76
  %|███████▌  | 21.2M/27.9M [01:12<00:20, 324k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 26; Resources: 21.0 CPU, 974.1MB object store:  76
  %|███████▋  | 21.2M/27.7M [01:12<00:20, 324k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 26; Resources: 21.0 CPU, 974.1MB object store:  77
  %|███████▋  | 21.4M/27.7M [01:12<00:21, 292k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 26; Resources: 21.0 CPU, 974.1MB object store:  77
  %|███████▋  | 21.4M/27.7M [01:12<00:21, 292k row/s]
  
  
  - Write: Tasks: 11; Actors: 0; Queued blocks: 0; Resources: 11.0 CPU, 1.8KB object store:  68%|██████▊   | 137/200 [01:12<00
  :18, 3.41 row/s]
  - Write: Tasks: 11; Actors: 0; Queued blocks: 0; Resources: 11.0 CPU, 1.8KB object store:  72%|███████▏  | 143/200 [01:12<00
  Running Dataset: dataset_15_0. Active & requested resources: 65/70 CPU, 1.0GB/39.9GB object store:  68%|██████▊   | 137/200 [01:13<00:18, 3.50 row/s]   0; Queued blocks: 0; Resources: 11.0 CPU, 1.8KB object store:  72%|███████▏  | 143/200 [01:12<00
  Running Dataset: dataset_15_0. Active & requested resources: 65/70 CPU, 1.0GB/39.9GB object store:  71%|███████   | 142/200 [01:13<00:15, 3.86 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 65/70 CPU, 1.0GB/39.9GB object store:  71%|███████   | 142/200 [01:13<00:15, 3.86 row/s]
  
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 4.5MB
   object store; [all objects local]:  76%|███████▌  | 21.2M/27.9M [01:12<00:18, 356k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 4.5MB
   object store; [all objects local]:  76%|███████▌  | 21.2M/27.8M [01:12<00:18, 356k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 4.5MB
   object store; [all objects local]:  77%|███████▋  | 21.3M/27.8M [01:12<00:23, 280k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 4.5MB
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 19; Resources: 21.0 CPU, 962.1MB object store:  77
  %|███████▋  | 21.4M/27.7M [01:13<00:21, 292k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 19; Resources: 21.0 CPU, 962.1MB object store:  78
  %|███████▊  | 21.4M/27.5M [01:13<00:20, 292k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 19; Resources: 21.0 CPU, 962.1MB object store:  80
  %|████████  | 22.0M/27.5M [01:13<00:15, 361k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 19; Resources: 21.0 CPU, 962.1MB object store:  80
  Running Dataset: dataset_15_0. Active & requested resources: 67/70 CPU, 952.6MB/39.9GB object store:  71%|███████   | 142/200 [01:14<00:15, 3.86 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 67/70 CPU, 952.6MB/39.9GB object store:  74%|███████▍  | 148/200 [01:14<00:11, 4.38 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 67/70 CPU, 952.6MB/39.9GB object store:  74%|███████▍  | 148/200 [01:14<00:11, 4.38 row/s]
  
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.1MB
   object store; [all objects local]:  77%|███████▋  | 21.3M/27.8M [01:13<00:23, 280k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.1MB
   object store; [all objects local]:  77%|███████▋  | 21.3M/27.5M [01:13<00:22, 280k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.1MB
   object store; [all objects local]:  80%|████████  | 22.0M/27.5M [01:13<00:14, 389k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.1MB
   object store; [all objects local]:  80%|████████  | 22.0M/27.5M [01:13<00:14, 389k row/s]
  
  - Write: Tasks: 12; Actors: 0; Queued blocks: 0; Resources: 12.0 CPU, 2.0KB object store:  72%|███████▏  | 143/200 [01:13<00
  :13, 4.08 row/s]
  - Write: Tasks: 12; Actors: 0; Queued blocks: 0; Resources: 12.0 CPU, 2.0KB object store:  74%|███████▍  | 148/200 [01:13<00
  :12, 4.24 row/s]
  - ReadParquet: Tasks: 20 [backpressured:tasks]; Actors: 0; Queued blocks: 16; Resources: 20.0 CPU, 943.5MB object store:  80
  %|████████  | 22.0M/27.5M [01:14<00:15, 361k row/s]
  - ReadParquet: Tasks: 20 [backpressured:tasks]; Actors: 0; Queued blocks: 16; Resources: 20.0 CPU, 943.5MB object store:  81
  %|████████  | 22.0M/27.3M [01:14<00:14, 361k row/s]
  - ReadParquet: Tasks: 20 [backpressured:tasks]; Actors: 0; Queued blocks: 16; Resources: 20.0 CPU, 943.5MB object store:  82
  %|████████▏ | 22.4M/27.3M [01:14<00:13, 365k row/s]
  - ReadParquet: Tasks: 20 [backpressured:tasks]; Actors: 0; Queued blocks: 16; Resources: 20.0 CPU, 943.5MB object store:  82
  %|████████▏ | 22.4M/27.3M [01:14<00:13, 365k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.9MB
   object store; [all objects local]:  80%|████████  | 22.0M/27.5M [01:14<00:14, 389k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.9MB
   object store; [all objects local]:  81%|████████  | 22.0M/27.3M [01:14<00:13, 389k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.9MB
   object store; [all objects local]:  82%|████████▏ | 22.4M/27.3M [01:14<00:12, 386k row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 902.3MB/39.9GB object store:  74%|███████▍  | 148/200 [01:15<00:11, 4.38 row/s] local]:  82%|████████▏ | 22.4M/27.3M [01:14<00:12, 386k row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 902.3MB/39.9GB object store:  74%|███████▍  | 149/200 [01:15<00:15, 3.35 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 902.3MB/39.9GB object store:  74%|███████▍  | 149/200 [01:15<00:15, 3.35 row/s]
  
  
  
  - Write: Tasks: 15; Actors: 0; Queued blocks: 0; Resources: 15.0 CPU, 2.5KB object store:  74%|███████▍  | 148/200 [01:14<00
  :12, 4.24 row/s]
  - Write: Tasks: 15; Actors: 0; Queued blocks: 0; Resources: 15.0 CPU, 2.5KB object store:  74%|███████▍  | 149/200 [01:14<00
  :15, 3.26 row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 14; Resources: 21.0 CPU, 954.1MB object store:  82
  %|████████▏ | 22.4M/27.3M [01:15<00:13, 365k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 14; Resources: 21.0 CPU, 954.1MB object store:  82
  %|████████▏ | 22.4M/27.3M [01:15<00:13, 365k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 14; Resources: 21.0 CPU, 954.1MB object store:  83
  %|████████▎ | 22.5M/27.3M [01:15<00:16, 290k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 14; Resources: 21.0 CPU, 954.1MB object store:  83
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 990.5MB/39.9GB object store:  74%|███████▍  | 149/200 [01:17<00:15, 3.35 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 990.5MB/39.9GB object store:  76%|███████▋  | 153/200 [01:17<00:13, 3.48 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 990.5MB/39.9GB object store:  76%|███████▋  | 153/200 [01:17<00:13, 3.48 row/s]
  
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.1MB
   object store; [all objects local]:  82%|████████▏ | 22.4M/27.3M [01:15<00:12, 386k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.1MB
   object store; [all objects local]:  82%|████████▏ | 22.4M/27.3M [01:15<00:12, 386k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.1MB
   object store; [all objects local]:  83%|████████▎ | 22.5M/27.3M [01:15<00:15, 300k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.1MB
   object store; [all objects local]:  83%|████████▎ | 22.5M/27.3M [01:15<00:15, 300k row/s]
  
  - Write: Tasks: 12; Actors: 0; Queued blocks: 0; Resources: 12.0 CPU, 2.0KB object store:  74%|███████▍  | 149/200 [01:15<00
  :15, 3.26 row/s]
  - Write: Tasks: 12; Actors: 0; Queued blocks: 0; Resources: 12.0 CPU, 2.0KB object store:  76%|███████▋  | 153/200 [01:15<00
  :13, 3.42 row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 9; Resources: 21.0 CPU, 946.7MB object store:  83%
  |████████▎ | 22.5M/27.3M [01:16<00:16, 290k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 9; Resources: 21.0 CPU, 946.7MB object store:  83%
  |████████▎ | 22.5M/27.0M [01:16<00:15, 290k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 9; Resources: 21.0 CPU, 946.7MB object store:  85%
  |████████▌ | 23.1M/27.0M [01:16<00:10, 367k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 9; Resources: 21.0 CPU, 946.7MB object store:  85%
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 939.0MB/39.9GB object store:  76%|███████▋  | 153/200 [01:18<00:13, 3.48 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 939.0MB/39.9GB object store:  78%|███████▊  | 157/200 [01:18<00:12, 3.55 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 939.0MB/39.9GB object store:  78%|███████▊  | 157/200 [01:18<00:12, 3.55 row/s]
  
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.5MB
   object store; [all objects local]:  83%|████████▎ | 22.5M/27.3M [01:16<00:15, 300k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.5MB
   object store; [all objects local]:  83%|████████▎ | 22.5M/27.1M [01:16<00:15, 300k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.5MB
   object store; [all objects local]:  85%|████████▍ | 23.0M/27.1M [01:16<00:11, 349k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.5MB
   object store; [all objects local]:  85%|████████▍ | 23.0M/27.1M [01:16<00:11, 349k row/s]
  
  - Write: Tasks: 13; Actors: 0; Queued blocks: 0; Resources: 13.0 CPU, 2.1KB object store:  76%|███████▋  | 153/200 [01:16<00
  :13, 3.42 row/s]
  - Write: Tasks: 13; Actors: 0; Queued blocks: 0; Resources: 13.0 CPU, 2.1KB object store:  78%|███████▊  | 157/200 [01:16<00
  :12, 3.51 row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 5; Resources: 21.0 CPU, 1007.6MB object store:  85
  %|████████▌ | 23.1M/27.0M [01:17<00:10, 367k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 5; Resources: 21.0 CPU, 1007.6MB object store:  86
  %|████████▌ | 23.1M/26.8M [01:17<00:10, 367k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 5; Resources: 21.0 CPU, 1007.6MB object store:  88
  %|████████▊ | 23.5M/26.8M [01:17<00:08, 370k row/s]
  - ReadParquet: Tasks: 21 [backpressured:tasks]; Actors: 0; Queued blocks: 5; Resources: 21.0 CPU, 1007.6MB object store:  88
  %|████████▊ | 23.5M/26.8M [01:17<00:08, 370k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 4; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.6MB
   object store; [all objects local]:  85%|████████▍ | 23.0M/27.1M [01:18<00:11, 349k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 4; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.6MB
   object store; [all objects local]:  85%|████████▌ | 23.0M/26.9M [01:18<00:11, 349k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 4; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.6MB
   object store; [all objects local]:  87%|████████▋ | 23.3M/26.9M [01:18<00:11, 329k row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 1.0GB/39.9GB object store:  78%|███████▊  | 157/200 [01:19<00:12, 3.55 row/s]   local]:  87%|████████▋ | 23.3M/26.9M [01:18<00:11, 329k row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 1.0GB/39.9GB object store:  80%|████████  | 160/200 [01:19<00:12, 3.32 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 1.0GB/39.9GB object store:  80%|████████  | 160/200 [01:19<00:12, 3.32 row/s]
  
  
  
  - Write: Tasks: 13; Actors: 0; Queued blocks: 0; Resources: 13.0 CPU, 2.1KB object store:  80%|████████  | 160/200 [01:18<00
  :12, 3.29 row/s]
  - ReadParquet: Tasks: 19 [backpressured:tasks]; Actors: 0; Queued blocks: 1; Resources: 19.0 CPU, 877.4MB object store:  88%
  |████████▊ | 23.5M/26.8M [01:18<00:08, 370k row/s]
  - ReadParquet: Tasks: 19 [backpressured:tasks]; Actors: 0; Queued blocks: 1; Resources: 19.0 CPU, 877.4MB object store:  88%
  |████████▊ | 23.5M/26.7M [01:18<00:08, 370k row/s]
  - ReadParquet: Tasks: 19 [backpressured:tasks]; Actors: 0; Queued blocks: 1; Resources: 19.0 CPU, 877.4MB object store:  90%
  |████████▉ | 24.0M/26.7M [01:18<00:06, 397k row/s]
  - ReadParquet: Tasks: 19 [backpressured:tasks]; Actors: 0; Queued blocks: 1; Resources: 19.0 CPU, 877.4MB object store:  90%
  |████████▉ | 24.0M/26.7M [01:18<00:06, 397k row/s]
  
  
  - Write: Tasks: 15; Actors: 0; Queued blocks: 0; Resources: 15.0 CPU, 2.5KB object store:  80%|████████  | 160/200 [01:19<00
  :12, 3.29 row/s]
  - Write: Tasks: 15; Actors: 0; Queued blocks: 0; Resources: 15.0 CPU, 2.5KB object store:  82%|████████▎ | 165/200 [01:19<00
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 883.3MB/39.9GB object store:  80%|████████  | 160/200 [01:20<00:12, 3.32 row/s] 0; Queued blocks: 0; Resources: 15.0 CPU, 2.5KB object store:  82%|████████▎ | 165/200 [01:19<00
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 883.3MB/39.9GB object store:  82%|████████▏ | 164/200 [01:20<00:10, 3.45 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 68/70 CPU, 883.3MB/39.9GB object store:  82%|████████▏ | 164/200 [01:20<00:10, 3.45 row/s]
  
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.8MB
   object store; [all objects local]:  87%|████████▋ | 23.3M/26.9M [01:19<00:11, 329k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.8MB
   object store; [all objects local]:  87%|████████▋ | 23.3M/26.7M [01:19<00:10, 329k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.8MB
   object store; [all objects local]:  89%|████████▉ | 23.9M/26.7M [01:19<00:07, 398k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.8MB
  - ReadParquet: Tasks: 18; Actors: 0; Queued blocks: 0; Resources: 18.0 CPU, 797.5MB object store:  90%|████████▉ | 24.0M/26.
  7M [01:20<00:06, 397k row/s]
  - ReadParquet: Tasks: 18; Actors: 0; Queued blocks: 0; Resources: 18.0 CPU, 797.5MB object store:  90%|█████████ | 24.0M/26.
  6M [01:20<00:06, 397k row/s]
  - ReadParquet: Tasks: 18; Actors: 0; Queued blocks: 0; Resources: 18.0 CPU, 797.5MB object store:  92%|█████████▏| 24.3M/26.
  6M [01:20<00:06, 364k row/s]
  - ReadParquet: Tasks: 18; Actors: 0; Queued blocks: 0; Resources: 18.0 CPU, 797.5MB object store:  92%|█████████▏| 24.3M/26.
  Running Dataset: dataset_15_0. Active & requested resources: 65/70 CPU, 779.4MB/39.9GB object store:  82%|████████▏ | 164/200 [01:21<00:10, 3.45 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 65/70 CPU, 779.4MB/39.9GB object store:  84%|████████▍ | 168/200 [01:21<00:09, 3.55 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 65/70 CPU, 779.4MB/39.9GB object store:  84%|████████▍ | 168/200 [01:21<00:09, 3.55 row/s]
  
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.4MB
   object store; [all objects local]:  89%|████████▉ | 23.9M/26.7M [01:20<00:07, 398k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.4MB
   object store; [all objects local]:  90%|████████▉ | 23.9M/26.6M [01:20<00:06, 398k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.4MB
   object store; [all objects local]:  91%|█████████ | 24.2M/26.6M [01:20<00:06, 364k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.4MB
   object store; [all objects local]:  91%|█████████ | 24.2M/26.6M [01:20<00:06, 364k row/s]
  
  - Write: Tasks: 14; Actors: 0; Queued blocks: 0; Resources: 14.0 CPU, 2.3KB object store:  82%|████████▎ | 165/200 [01:20<00
  :09, 3.71 row/s]
  - Write: Tasks: 14; Actors: 0; Queued blocks: 0; Resources: 14.0 CPU, 2.3KB object store:  84%|████████▍ | 168/200 [01:20<00
  :09, 3.45 row/s]
  - ReadParquet: Tasks: 16; Actors: 0; Queued blocks: 0; Resources: 16.0 CPU, 706.9MB object store:  92%|█████████▏| 24.3M/26.
  6M [01:21<00:06, 364k row/s]
  - ReadParquet: Tasks: 16; Actors: 0; Queued blocks: 0; Resources: 16.0 CPU, 706.9MB object store:  92%|█████████▏| 24.3M/26.
  5M [01:21<00:06, 364k row/s]
  - ReadParquet: Tasks: 16; Actors: 0; Queued blocks: 0; Resources: 16.0 CPU, 706.9MB object store:  92%|█████████▏| 24.4M/26.
  5M [01:21<00:07, 285k row/s]
  - ReadParquet: Tasks: 16; Actors: 0; Queued blocks: 0; Resources: 16.0 CPU, 706.9MB object store:  92%|█████████▏| 24.4M/26.
  5M [01:21<00:07, 285k row/s]
  
  
  - Write: Tasks: 13; Actors: 0; Queued blocks: 0; Resources: 13.0 CPU, 2.1KB object store:  84%|████████▍ | 168/200 [01:21<00
  :09, 3.45 row/s]
  - Write: Tasks: 13; Actors: 0; Queued blocks: 0; Resources: 13.0 CPU, 2.1KB object store:  86%|████████▌ | 172/200 [01:21<00
  Running Dataset: dataset_15_0. Active & requested resources: 64/70 CPU, 711.9MB/39.9GB object store:  84%|████████▍ | 168/200 [01:22<00:09, 3.55 row/s] 0; Queued blocks: 0; Resources: 13.0 CPU, 2.1KB object store:  86%|████████▌ | 172/200 [01:21<00
  Running Dataset: dataset_15_0. Active & requested resources: 64/70 CPU, 711.9MB/39.9GB object store:  86%|████████▌ | 172/200 [01:22<00:07, 3.63 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 64/70 CPU, 711.9MB/39.9GB object store:  86%|████████▌ | 172/200 [01:22<00:07, 3.63 row/s]
  
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.6MB
   object store; [all objects local]:  91%|█████████ | 24.2M/26.6M [01:21<00:06, 364k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.6MB
   object store; [all objects local]:  91%|█████████ | 24.2M/26.5M [01:21<00:06, 364k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.6MB
   object store; [all objects local]:  92%|█████████▏| 24.4M/26.5M [01:21<00:06, 312k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.6MB
  - ReadParquet: Tasks: 11; Actors: 0; Queued blocks: 0; Resources: 11.0 CPU, 549.5MB object store:  92%|█████████▏| 24.4M/26.
  5M [01:22<00:07, 285k row/s]
  - ReadParquet: Tasks: 11; Actors: 0; Queued blocks: 0; Resources: 11.0 CPU, 549.5MB object store:  93%|█████████▎| 24.4M/26.
  3M [01:22<00:06, 285k row/s]
  - ReadParquet: Tasks: 11; Actors: 0; Queued blocks: 0; Resources: 11.0 CPU, 549.5MB object store:  95%|█████████▍| 24.9M/26.
  3M [01:22<00:04, 341k row/s]
  - ReadParquet: Tasks: 11; Actors: 0; Queued blocks: 0; Resources: 11.0 CPU, 549.5MB object store:  95%|█████████▍| 24.9M/26.
  3M [01:22<00:04, 341k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.6MB
   object store; [all objects local]:  92%|█████████▏| 24.4M/26.4M [01:22<00:06, 312k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.6MB
   object store; [all objects local]:  94%|█████████▍| 24.8M/26.4M [01:22<00:04, 332k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.6MB
   object store; [all objects local]:  94%|█████████▍| 24.8M/26.4M [01:22<00:04, 332k row/s]
  
  - Write: Tasks: 12; Actors: 0; Queued blocks: 0; Resources: 12.0 CPU, 2.0KB object store:  86%|████████▌ | 172/200 [01:22<00
  :07, 3.56 row/s]
  - Write: Tasks: 12; Actors: 0; Queued blocks: 0; Resources: 12.0 CPU, 2.0KB object store:  88%|████████▊ | 176/200 [01:22<00
  Running Dataset: dataset_15_0. Active & requested resources: 56/70 CPU, 510.1MB/39.9GB object store:  86%|████████▌ | 172/200 [01:23<00:07, 3.63 row/s] 0; Queued blocks: 0; Resources: 12.0 CPU, 2.0KB object store:  88%|████████▊ | 176/200 [01:22<00
  Running Dataset: dataset_15_0. Active & requested resources: 56/70 CPU, 510.1MB/39.9GB object store:  88%|████████▊ | 175/200 [01:23<00:07, 3.39 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 56/70 CPU, 510.1MB/39.9GB object store:  88%|████████▊ | 175/200 [01:23<00:07, 3.39 row/s]
  
  - ReadParquet: Tasks: 5; Actors: 0; Queued blocks: 0; Resources: 5.0 CPU, 251.2MB object store:  95%|█████████▍| 24.9M/26.3M
   [01:23<00:04, 341k row/s]
  - ReadParquet: Tasks: 5; Actors: 0; Queued blocks: 0; Resources: 5.0 CPU, 251.2MB object store:  95%|█████████▌| 24.9M/26.2M
   [01:23<00:03, 341k row/s]
  - ReadParquet: Tasks: 5; Actors: 0; Queued blocks: 0; Resources: 5.0 CPU, 251.2MB object store:  98%|█████████▊| 25.5M/26.2M
   [01:23<00:01, 407k row/s]
  - ReadParquet: Tasks: 5; Actors: 0; Queued blocks: 0; Resources: 5.0 CPU, 251.2MB object store:  98%|█████████▊| 25.5M/26.2M
  Running Dataset: dataset_15_0. Active & requested resources: 52/70 CPU, 246.2MB/39.9GB object store:  88%|████████▊ | 175/200 [01:24<00:07, 3.39 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 52/70 CPU, 246.2MB/39.9GB object store:  90%|█████████ | 181/200 [01:24<00:04, 4.06 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 52/70 CPU, 246.2MB/39.9GB object store:  90%|█████████ | 181/200 [01:24<00:04, 4.06 row/s]
  
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.0MB
   object store; [all objects local]:  94%|█████████▍| 24.8M/26.4M [01:23<00:04, 332k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.0MB
   object store; [all objects local]:  95%|█████████▍| 24.8M/26.2M [01:23<00:04, 332k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.0MB
   object store; [all objects local]:  97%|█████████▋| 25.4M/26.2M [01:23<00:01, 401k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 2; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 6.0MB
   object store; [all objects local]:  97%|█████████▋| 25.4M/26.2M [01:23<00:01, 401k row/s]
  
  - Write: Tasks: 13; Actors: 0; Queued blocks: 0; Resources: 13.0 CPU, 2.1KB object store:  88%|████████▊ | 176/200 [01:23<00
  :06, 3.62 row/s]
  - Write: Tasks: 13; Actors: 0; Queued blocks: 0; Resources: 13.0 CPU, 2.1KB object store:  90%|█████████ | 181/200 [01:23<00
  :04, 3.94 row/s]
  - ReadParquet: Tasks: 1; Actors: 0; Queued blocks: 0; Resources: 1.0 CPU, 76.7MB object store:  98%|█████████▊| 25.5M/26.2M
  [01:24<00:01, 407k row/s]
  - ReadParquet: Tasks: 1; Actors: 0; Queued blocks: 0; Resources: 1.0 CPU, 76.7MB object store:  98%|█████████▊| 25.5M/26.0M
  [01:24<00:01, 407k row/s]
  - ReadParquet: Tasks: 1; Actors: 0; Queued blocks: 0; Resources: 1.0 CPU, 76.7MB object store:  99%|█████████▉| 25.9M/26.0M
  [01:24<00:00, 398k row/s]
  - ReadParquet: Tasks: 1; Actors: 0; Queued blocks: 0; Resources: 1.0 CPU, 76.7MB object store:  99%|█████████▉| 25.9M/26.0M
  [01:24<00:00, 398k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.8MB
   object store; [all objects local]:  97%|█████████▋| 25.4M/26.2M [01:24<00:01, 401k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.8MB
   object store; [all objects local]:  98%|█████████▊| 25.4M/26.0M [01:24<00:01, 401k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 35; Queued blocks: 0; Resources: 35.0 CPU, 5.8MB
   object store; [all objects local]:  99%|█████████▉| 25.9M/26.0M [01:24<00:00, 423k row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 50/70 CPU, 49.1MB/39.9GB object store:  90%|█████████ | 181/200 [01:25<00:04, 4.06 row/s]  local]:  99%|█████████▉| 25.9M/26.0M [01:24<00:00, 423k row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 50/70 CPU, 49.1MB/39.9GB object store:  92%|█████████▏| 184/200 [01:25<00:04, 3.69 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 50/70 CPU, 49.1MB/39.9GB object store:  92%|█████████▏| 184/200 [01:25<00:04, 3.69 row/s]
  
  
  
  - Write: Tasks: 15; Actors: 0; Queued blocks: 0; Resources: 15.0 CPU, 2.5KB object store:  90%|█████████ | 181/200 [01:24<00
  :04, 3.94 row/s]
  - Write: Tasks: 15; Actors: 0; Queued blocks: 0; Resources: 15.0 CPU, 2.5KB object store:  92%|█████████▏| 184/200 [01:24<00
  :04, 3.61 row/s]
  - ReadParquet: Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 33.3MB object store:  99%|█████████▉| 25.9M/26.0M
  [01:25<00:00, 398k row/s]
  - ReadParquet: Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 33.3MB object store: 100%|█████████▉| 25.9M/26.0M
  [01:25<00:00, 398k row/s]
  - ReadParquet: Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 33.3MB object store: 100%|██████████| 26.0M/26.0M
  [01:25<00:00, 309k row/s]
  - ReadParquet: Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 33.3MB object store: 100%|██████████| 26.0M/26.0M
  [01:25<00:00, 309k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 32; Queued blocks: 0; Resources: 33.0 CPU, 5.5MB
   object store; [all objects local]:  99%|█████████▉| 25.9M/26.0M [01:25<00:00, 423k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 32; Queued blocks: 0; Resources: 33.0 CPU, 5.5MB
   object store; [all objects local]: 100%|█████████▉| 25.9M/26.0M [01:25<00:00, 423k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 1; Actors: 32; Queued blocks: 0; Resources: 33.0 CPU, 5.5MB
   object store; [all objects local]: 100%|██████████| 26.0M/26.0M [01:25<00:00, 326k row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 45/70 CPU, 5.3MB/39.9GB object store:  92%|█████████▏| 184/200 [01:26<00:04, 3.69 row/s] s local]: 100%|██████████| 26.0M/26.0M [01:25<00:00, 326k row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 45/70 CPU, 5.3MB/39.9GB object store:  93%|█████████▎| 186/200 [01:26<00:04, 3.17 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 45/70 CPU, 5.3MB/39.9GB object store:  93%|█████████▎| 186/200 [01:26<00:04, 3.17 row/s]
  
  
  
  - Write: Tasks: 14; Actors: 0; Queued blocks: 0; Resources: 14.0 CPU, 2.3KB object store:  92%|█████████▏| 184/200 [01:25<00
  :04, 3.61 row/s]
  - Write: Tasks: 14; Actors: 0; Queued blocks: 0; Resources: 14.0 CPU, 2.3KB object store:  93%|█████████▎| 186/200 [01:25<00
  :04, 3.11 row/s]
  - ReadParquet: Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 0.0B object store: 100%|██████████| 26.0M/26.0M [0
  1:26<00:00, 309k row/s]
  - ReadParquet: Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 0.0B object store: 100%|██████████| 26.0M/26.0M [0
  1:26<00:00, 309k row/s]
  
  
  - Write: Tasks: 11; Actors: 0; Queued blocks: 0; Resources: 11.0 CPU, 2.0KB object store:  93%|█████████▎| 186/200 [01:26<00
  :04, 3.11 row/s]
  - Write: Tasks: 11; Actors: 0; Queued blocks: 0; Resources: 11.0 CPU, 2.0KB object store:  95%|█████████▌| 190/200 [01:26<00
  Running Dataset: dataset_15_0. Active & requested resources: 31/70 CPU, 3.4MB/39.9GB object store:  93%|█████████▎| 186/200 [01:27<00:04, 3.17 row/s]s: 0; Queued blocks: 0; Resources: 11.0 CPU, 2.0KB object store:  95%|█████████▌| 190/200 [01:26<00
  Running Dataset: dataset_15_0. Active & requested resources: 31/70 CPU, 3.4MB/39.9GB object store:  94%|█████████▍| 189/200 [01:27<00:03, 3.08 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 31/70 CPU, 3.4MB/39.9GB object store:  94%|█████████▍| 189/200 [01:27<00:03, 3.08 row/s]
  
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 21; Queued blocks: 0; Resources: 22.0 CPU, 3.4MB
   object store; [all objects local]: 100%|██████████| 26.0M/26.0M [01:26<00:00, 326k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 21; Queued blocks: 0; Resources: 22.0 CPU, 3.4MB
  - ReadParquet: Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 0.0B object store: 100%|██████████| 26.0M/26.0M [0
  Running Dataset: dataset_15_0. Active & requested resources: 16/70 CPU, 1.5MB/39.9GB object store:  94%|█████████▍| 189/200 [01:28<00:03, 3.08 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 16/70 CPU, 1.5MB/39.9GB object store:  98%|█████████▊| 196/200 [01:28<00:00, 4.17 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 16/70 CPU, 1.5MB/39.9GB object store:  98%|█████████▊| 196/200 [01:28<00:00, 4.17 row/s]
  
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 11; Queued blocks: 0; Resources: 12.0 CPU, 1.5MB
   object store; [all objects local]: 100%|██████████| 26.0M/26.0M [01:27<00:00, 326k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 11; Queued blocks: 0; Resources: 12.0 CPU, 1.5MB
   object store; [all objects local]: 100%|██████████| 26.0M/26.0M [01:27<00:00, 326k row/s]
  
  - Write: Tasks: 4; Actors: 0; Queued blocks: 0; Resources: 4.0 CPU, 672.0B object store:  95%|█████████▌| 190/200 [01:27<00:
  03, 3.33 row/s]
  - Write: Tasks: 4; Actors: 0; Queued blocks: 0; Resources: 4.0 CPU, 672.0B object store:  98%|█████████▊| 196/200 [01:27<00:
  00, 4.06 row/s]
  - ReadParquet: Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 0.0B object store: 100%|██████████| 26.0M/26.0M [0
  Running Dataset: dataset_15_0. Active & requested resources: 3/70 CPU, 390.8KB/39.9GB object store:  98%|█████████▊| 196/200 [01:29<00:00, 4.17 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 3/70 CPU, 390.8KB/39.9GB object store: 100%|█████████▉| 199/200 [01:29<00:00, 3.79 row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 3/70 CPU, 390.8KB/39.9GB object store: 100%|█████████▉| 199/200 [01:29<00:00, 3.79 row/s]
  
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 1; Queued blocks: 0; Resources: 2.0 CPU, 390.6KB
   object store; [all objects local]: 100%|██████████| 26.0M/26.0M [01:28<00:00, 326k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 1; Queued blocks: 0; Resources: 2.0 CPU, 390.6KB
   object store; [all objects local]: 100%|██████████| 26.0M/26.0M [01:28<00:00, 326k row/s]
  
  - Write: Tasks: 1; Actors: 0; Queued blocks: 0; Resources: 1.0 CPU, 168.0B object store:  98%|█████████▊| 196/200 [01:28<00:
  00, 4.06 row/s]
  - Write: Tasks: 1; Actors: 0; Queued blocks: 0; Resources: 1.0 CPU, 168.0B object store: 100%|█████████▉| 199/200 [01:28<00:
  00, 3.71 row/s]
  - ReadParquet: Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 0.0B object store: 100%|██████████| 26.0M/26.0M [0
  1:29<00:00, 309k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 390.6KB
   object store; [all objects local]: 100%|██████████| 26.0M/26.0M [01:29<00:00, 326k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 390.6KB
   object store; [all objects local]: 100%|██████████| 26.0M/26.0M [01:29<00:00, 326k row/s]
  Running Dataset: dataset_15_0. Active & requested resources: 1/70 CPU, 390.8KB/39.9GB object store: 100%|█████████▉| 199/200 [01:30<00:00, 3.79 row/s] 0; Queued blocks: 0; Resources: 1.0 CPU, 168.0B object store: 100%|█████████▉| 199/200 [01:29<00:
  Running Dataset: dataset_15_0. Active & requested resources: 1/70 CPU, 390.8KB/39.9GB object store: 100%|█████████▉| 199/200 [01:30<00:00, 3.79 row/s]2025-10-14 07:24:07,271       INFO streaming_executor.py:279 -- ✔️  Dataset dataset_15_0 execution finished in 90.81 seconds
  
  ✔️  Dataset dataset_15_0 execution finished in 90.81 seconds: 100%|█████████▉| 199/200 [01:30<00:00, 3.79 row/s]            
  ✔️  Dataset dataset_15_0 execution finished in 90.81 seconds: 100%|██████████| 200/200 [01:30<00:00, 2.91 row/s]
  ✔️  Dataset dataset_15_0 execution finished in 90.81 seconds: 100%|██████████| 200/200 [01:30<00:00, 2.91 row/s]
  
  
                                                                                                                              
  
  
  
  
  
  
  ✔️  Dataset dataset_15_0 execution finished in 90.81 seconds: 100%|██████████| 200/200 [01:30<00:00, 2.20 row/s]
  
  
  - ReadParquet: Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 0.0B object store: 100%|██████████| 26.0M/26.0M [0
  1:29<00:00, 309k row/s]
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 390.6KB
   object store; [all objects local]: 100%|██████████| 26.0M/26.0M [01:29<00:00, 326k row/s]
  
  - ReadParquet: Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 0.0B object store: 100%|██████████| 26.0M/26.0M [0
  1:29<00:00, 309k row/s]
                                                                                                                              
  
  
  
  
  
  
  - ReadParquet: Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 0.0B object store: 100%|██████████| 26.0M/26.0M [01:29<00:00, 290k row/s]
  
  
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 390.6KB
   object store; [all objects local]: 100%|██████████| 26.0M/26.0M [01:29<00:00, 326k row/s]
  
  - Write: Tasks: 1; Actors: 0; Queued blocks: 0; Resources: 1.0 CPU, 168.0B object store: 100%|█████████▉| 199/200 [01:29<00:
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 0.0B ob
  ject store; [all objects local]: 100%|██████████| 26.0M/26.0M [01:29<00:00, 326k row/s]
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 0.0B ob
  ject store; [all objects local]: 100%|██████████| 26.0M/26.0M [01:29<00:00, 326k row/s]
  
  
  
  
  - MapBatches(drop_columns)->MapBatches(XGBoostPredictor): Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 0.0B object store; [all objects local]: 100%|██████████| 26.0M/26.0M [01:29<00:00, 290k row/s]
  
  
  
  
  - Write: Tasks: 1; Actors: 0; Queued blocks: 0; Resources: 1.0 CPU, 168.0B object store: 100%|█████████▉| 199/200 [01:29<00:
  00, 3.71 row/s]
  - Write: Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 168.0B object store: 100%|█████████▉| 199/200 [01:29<00:
  00, 3.71 row/s]
  - Write: Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 168.0B object store: 100%|██████████| 200/200 [01:29<00:
  00, 2.85 row/s]
  - Write: Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 168.0B object store: 100%|██████████| 200/200 [01:29<00:
  00, 2.85 row/s]
  - Write: Tasks: 0; Actors: 0; Queued blocks: 0; Resources: 0.0 CPU, 168.0B object store: 100%|██████████| 200/200 [01:29<00:00, 2.23 row/s]
  2025-10-14 07:24:07,378 INFO dataset.py:4981 -- Data sink Parquet finished. 26000000 rows and 99.2MB data written.
  Training result:
   Result(
    metrics={'train-logloss': 0.4161404912044953, 'train-error': 0.14671265384615384},
    path='traineddata/cluster_storage/xgboost_benchmark/XGBoostTrainer_f8a55_00000_0_2025-10-14_07-20-46',
    filesystem='abfs',
    checkpoint=Checkpoint(filesystem=abfs, path=traineddata/cluster_storage/xgboost_benchmark/XGBoostTrainer_f8a55_00000_0_2025-10-14_07-20-46/checkpoint_000000)
  )
  Training/prediction times: {'training_time': 134.46404198399978, 'prediction_time': 108.36333002202446}
  2025-10-14 07:24:12,397 SUCC cli.py:65 -- --------------------------------------
  2025-10-14 07:24:12,397 SUCC cli.py:66 -- Job 'rayjob-tune-gpt2-bh9hz' succeeded
  2025-10-14 07:24:12,397 SUCC cli.py:67 -- --------------------------------------
    ``` 
  
  
  **Using ADLFS Interface**:
    This method uses [adlfs.AzureBlobFileSystem](https://fsspec.github.io/adlfs/api/#adlfs.AzureBlobFileSystem) for reading the parquet files from Azure Blob. For writing checkpoints to remote storage ray uses [Runconfig](https://docs.ray.io/en/latest/train/api/doc/ray.train.RunConfig.html#ray-train-runconfig) in which **storage_path** argument determines location checkpoints are persisted and **storage_filesystem** argument expects only of [Pyarrow Filesystem](https://arrow.apache.org/docs/python/generated/pyarrow.fs.FileSystem.html#pyarrow.fs.FileSystem) type.
  
  
  Below is the output where adlfs adapter is used in Ray Xgboost inference model:
  ```sh
  mittas@MITTAS-WORKLAP:~/workspace/azure/aks-ray-sample/sample-tuning-setup/direct-blob-access$ kubectl  logs -f rayjob-tune-gpt2-827nk -n kuberay
  2025-10-14 04:17:27,620 - INFO - Note: NumExpr detected 16 cores but "NUMEXPR_MAX_THREADS" not set, so enforcing safe limit of 8.
  2025-10-14 04:17:27,620 - INFO - NumExpr defaulting to 8 threads.
  2025-10-14 04:17:28,205 INFO cli.py:41 -- Job submission server address: http://rayjob-tune-gpt2-vrmb9-head-svc.kuberay.svc.cluster.local:8265
  2025-10-14 04:17:29,297 SUCC cli.py:65 -- ---------------------------------------------------
  2025-10-14 04:17:29,297 SUCC cli.py:66 -- Job 'rayjob-tune-gpt2-gpkg7' submitted successfully
  2025-10-14 04:17:29,297 SUCC cli.py:67 -- ---------------------------------------------------
  2025-10-14 04:17:29,297 INFO cli.py:291 -- Next steps
  2025-10-14 04:17:29,297 INFO cli.py:292 -- Query the logs of the job:
  2025-10-14 04:17:29,297 INFO cli.py:294 -- ray job logs rayjob-tune-gpt2-gpkg7
  2025-10-14 04:17:29,297 INFO cli.py:296 -- Query the status of the job:
  2025-10-14 04:17:29,297 INFO cli.py:298 -- ray job status rayjob-tune-gpt2-gpkg7
  2025-10-14 04:17:29,298 INFO cli.py:300 -- Request the job to be stopped:
  2025-10-14 04:17:29,298 INFO cli.py:302 -- ray job stop rayjob-tune-gpt2-gpkg7
  2025-10-14 04:17:30,649 - INFO - Note: NumExpr detected 16 cores but "NUMEXPR_MAX_THREADS" not set, so enforcing safe limit of 8.
  2025-10-14 04:17:30,649 - INFO - NumExpr defaulting to 8 threads.
  2025-10-14 04:17:31,232 INFO cli.py:41 -- Job submission server address: http://rayjob-tune-gpt2-vrmb9-head-svc.kuberay.svc.cluster.local:8265
  2025-10-14 04:17:29,054 INFO job_manager.py:568 -- Runtime env is setting up.
  Running xgboost training benchmark...
  2025-10-14 04:17:46,511 INFO worker.py:1692 -- Using address 10.244.3.249:6379 set in the environment variable RAY_ADDRESS
  2025-10-14 04:17:46,514 INFO worker.py:1833 -- Connecting to existing Ray cluster at address: 10.244.3.249:6379...
  2025-10-14 04:17:46,530 INFO worker.py:2004 -- Connected to Ray cluster. View the dashboard at 10.244.3.249:8265
  /home/ray/anaconda3/lib/python3.10/site-packages/ray/_private/worker.py:2052: FutureWarning: Tip: In future versions of Ray, Ray will no longer override accelerator visible devices env var if num_gpus=0 or num_gpus=None (default). To enable this behavior and turn off this error message, set RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO=0
    warnings.warn(
  2025-10-14 04:17:46,615 INFO tune.py:253 -- Initializing Ray automatically. For cluster usage or custom Ray initialization, call `ray.init(...)` before `<FrameworkTrainer>(...)`.
  /home/ray/anaconda3/lib/python3.10/site-packages/google/rpc/__init__.py:18: UserWarning: pkg_resources is deprecated as an API. See https://setuptools.pypa.io/en/latest/pkg_resources.html. The pkg_resources package is slated for removal as early as 2025-11-30. Refrain from using this package or pin to Setuptools<81.
    import pkg_resources
  
  View detailed results here: traineddata/cluster_storage_adlfs/xgboost_benchmark
  To visualize your results with TensorBoard, run: `tensorboard --logdir /tmp/ray/session_2025-10-14_04-16-24_757462_1/artifacts/2025-10-14_04-17-48/xgboost_benchmark/driver_artifacts`
  
  Training started with configuration:
  ╭──────────────────────────────────────────────────────────────╮
  │ Training config                                              │
  ├──────────────────────────────────────────────────────────────┤
  │ train_loop_config/account_name               mittassaaccount │
  │ train_loop_config/data_path             ...10G-xgboost-data/ │
  │ train_loop_config/label_column                        labels │
  │ train_loop_config/params/eval_metric    ['logloss', 'error'] │
  │ train_loop_config/params/objective           binary:logistic │
  │ train_loop_config/report_callback_cls   ...nReportCallback'> │
  ╰──────────────────────────────────────────────────────────────╯
  (XGBoostTrainer pid=2249) Started distributed worker processes:
  (XGBoostTrainer pid=2249) - (node_id=ce89e1453dd2b97bec0ab5ae0282491ff3acdd551a3821bb6b0e6b35, ip=10.244.3.249, pid=2327) world_rank=0, local_rank=0, node_rank=0
  (XGBoostTrainer pid=2249) - (node_id=55bd1268729b6a573d6850e819ccb862fc6426510797bb5196a9453a, ip=10.244.1.122, pid=337) world_rank=1, local_rank=0, node_rank=1
  (XGBoostTrainer pid=2249) - (node_id=257733fb0aa1b4bd7d4aa97106b6e9730418abdca66837e73e5262a0, ip=10.244.2.110, pid=338) world_rank=2, local_rank=0, node_rank=2
  (XGBoostTrainer pid=2249) - (node_id=8e7ff08107fd0204c5da9d52639c90c310d3f826282b21b698b71e9c, ip=10.244.4.85, pid=321) world_rank=3, local_rank=0, node_rank=3
  (RayTrainWorker pid=321, ip=10.244.4.85) [04:17:57] Task [xgboost.ray-rank=00000003]:540fb6a29761167b22cbf39c02000000 got rank 3
  
  Training finished iteration 1 at 2025-10-14 04:25:23. Total running time: 7min 31s
  ╭───────────────────────────────╮
  │ Training result               │
  ├───────────────────────────────┤
  │ checkpoint_dir_name           │
  │ time_this_iter_s      448.393 │
  │ time_total_s          448.393 │
  │ training_iteration          1 │
  ╰───────────────────────────────╯
  
  Training finished iteration 2 at 2025-10-14 04:25:23. Total running time: 7min 32s
  ╭───────────────────────────────╮
  │ Training result               │
  ├───────────────────────────────┤
  │ checkpoint_dir_name           │
  │ time_this_iter_s      0.26011 │
  │ time_total_s          448.653 │
  │ training_iteration          2 │
  ╰───────────────────────────────╯
  
  Training finished iteration 3 at 2025-10-14 04:25:23. Total running time: 7min 32s
  ╭───────────────────────────────╮
  │ Training result               │
  ├───────────────────────────────┤
  │ checkpoint_dir_name           │
  │ time_this_iter_s      0.32502 │
  │ time_total_s          448.978 │
  │ training_iteration          3 │
  ╰───────────────────────────────╯
  
  Training finished iteration 4 at 2025-10-14 04:25:24. Total running time: 7min 32s
  ╭───────────────────────────────╮
  │ Training result               │
  ├───────────────────────────────┤
  │ checkpoint_dir_name           │
  │ time_this_iter_s        0.367 │
  │ time_total_s          449.345 │
  │ training_iteration          4 │
  ╰───────────────────────────────╯
  
  Training finished iteration 5 at 2025-10-14 04:25:24. Total running time: 7min 33s
  ╭───────────────────────────────╮
  │ Training result               │
  ├───────────────────────────────┤
  │ checkpoint_dir_name           │
  │ time_this_iter_s       0.3034 │
  │ time_total_s          449.648 │
  │ training_iteration          5 │
  ╰───────────────────────────────╯
  
  Training finished iteration 6 at 2025-10-14 04:25:24. Total running time: 7min 33s
  ╭───────────────────────────────╮
  │ Training result               │
  ├───────────────────────────────┤
  │ checkpoint_dir_name           │
  │ time_this_iter_s      0.31671 │
  │ time_total_s          449.965 │
  │ training_iteration          6 │
  ╰───────────────────────────────╯
  
  Training finished iteration 7 at 2025-10-14 04:25:25. Total running time: 7min 33s
  ╭───────────────────────────────╮
  │ Training result               │
  ├───────────────────────────────┤
  │ checkpoint_dir_name           │
  │ time_this_iter_s      0.32888 │
  │ time_total_s          450.294 │
  │ training_iteration          7 │
  ╰───────────────────────────────╯
  
  Training finished iteration 8 at 2025-10-14 04:25:25. Total running time: 7min 34s
  ╭───────────────────────────────╮
  │ Training result               │
  ├───────────────────────────────┤
  │ checkpoint_dir_name           │
  │ time_this_iter_s      0.32556 │
  │ time_total_s           450.62 │
  │ training_iteration          8 │
  ╰───────────────────────────────╯
  
  Training finished iteration 9 at 2025-10-14 04:25:25. Total running time: 7min 34s
  ╭───────────────────────────────╮
  │ Training result               │
  ├───────────────────────────────┤
  │ checkpoint_dir_name           │
  │ time_this_iter_s      0.31188 │
  │ time_total_s          450.931 │
  │ training_iteration          9 │
  ╰───────────────────────────────╯
  
  Training finished iteration 10 at 2025-10-14 04:25:26. Total running time: 7min 34s
  ╭───────────────────────────────╮
  │ Training result               │
  ├───────────────────────────────┤
  │ checkpoint_dir_name           │
  │ time_this_iter_s      0.32912 │
  │ time_total_s          451.261 │
  │ training_iteration         10 │
  ╰───────────────────────────────╯
  2025-10-14 04:25:29,107 ERROR tune_controller.py:1331 -- Trial task failed for trial XGBoostTrainer_6a896_00000
  Traceback (most recent call last):
    File "/home/ray/anaconda3/lib/python3.10/site-packages/ray/air/execution/_internal/event_manager.py", line 110, in resolve_future
      result = ray.get(future)
    File "/home/ray/anaconda3/lib/python3.10/site-packages/ray/_private/auto_init_hook.py", line 22, in auto_init_wrapper
      return fn(*args, **kwargs)
    File "/home/ray/anaconda3/lib/python3.10/site-packages/ray/_private/client_mode_hook.py", line 104, in wrapper
      return func(*args, **kwargs)
    File "/home/ray/anaconda3/lib/python3.10/site-packages/ray/_private/worker.py", line 2962, in get
      values, debugger_breakpoint = worker.get_objects(
    File "/home/ray/anaconda3/lib/python3.10/site-packages/ray/_private/worker.py", line 1026, in get_objects
      raise value.as_instanceof_cause()
  ray.exceptions.RayTaskError(RuntimeError): ray::_Inner.train() (pid=2249, ip=10.244.3.249, actor_id=1450a34748e2f78966d9891502000000, repr=XGBoostTrainer)
    File "/home/ray/anaconda3/lib/python3.10/site-packages/ray/tune/trainable/trainable.py", line 331, in train
      raise skipped from exception_cause(skipped)
    File "/home/ray/anaconda3/lib/python3.10/site-packages/ray/train/_internal/utils.py", line 57, in check_for_failure
      ray.get(object_ref)
  ray.exceptions.RayTaskError(RuntimeError): ray::_RayTrainWorker__execute.get_next() (pid=2327, ip=10.244.3.249, actor_id=f96bf06ce6e268b5342e5a9702000000, repr=<ray.train._internal.worker_group.RayTrainWorker object at 0x7efc4f0ed2a0>)
    File "/home/ray/anaconda3/lib/python3.10/site-packages/ray/train/_internal/worker_group.py", line 33, in __execute
      raise skipped from exception_cause(skipped)
    File "/home/ray/anaconda3/lib/python3.10/site-packages/ray/train/_internal/utils.py", line 176, in discard_return_wrapper
      train_func(*args, **kwargs)
    File "/tmp/ray/session_2025-10-14_04-16-24_757462_1/runtime_resources/working_dir_files/https_github_com_mittachaitu_aks-ray-sample_archive_sai_wi/sample-tuning-setup/direct-blob-access/train_batch_inference_adlfs.py", line 174, in xgboost_train_loop_function
      xgb.train(
    File "/home/ray/anaconda3/lib/python3.10/site-packages/xgboost/core.py", line 726, in inner_f
      return func(**kwargs)
    File "/home/ray/anaconda3/lib/python3.10/site-packages/xgboost/training.py", line 185, in train
      bst = cb_container.after_training(bst)
    File "/home/ray/anaconda3/lib/python3.10/site-packages/xgboost/callback.py", line 190, in after_training
      model = c.after_training(model=model)
    File "/home/ray/anaconda3/lib/python3.10/site-packages/ray/train/xgboost/_xgboost_utils.py", line 153, in after_training
      self._save_and_report_checkpoint(report_dict, model)
    File "/home/ray/anaconda3/lib/python3.10/site-packages/ray/train/xgboost/_xgboost_utils.py", line 249, in _save_and_report_checkpoint
      ray.train.report(report_dict, checkpoint=checkpoint)
    File "/home/ray/anaconda3/lib/python3.10/site-packages/ray/train/_internal/session.py", line 683, in wrapper
      return fn(*args, **kwargs)
    File "/home/ray/anaconda3/lib/python3.10/site-packages/ray/train/_internal/session.py", line 801, in report
      get_session().report(metrics, checkpoint=checkpoint)
    File "/home/ray/anaconda3/lib/python3.10/site-packages/ray/train/_internal/session.py", line 449, in report
      persisted_checkpoint = self.storage.persist_current_checkpoint(checkpoint)
    File "/home/ray/anaconda3/lib/python3.10/site-packages/ray/train/_internal/storage.py", line 547, in persist_current_checkpoint
      self._check_validation_file()
    File "/home/ray/anaconda3/lib/python3.10/site-packages/ray/train/_internal/storage.py", line 499, in _check_validation_file
      raise RuntimeError(
  RuntimeError: Unable to set up cluster storage with the following settings:
  StorageContext<
    storage_filesystem='abfs',
    storage_fs_path='traineddata/cluster_storage_adlfs',
    experiment_dir_name='xgboost_benchmark',
    trial_dir_name='XGBoostTrainer_6a896_00000_0_2025-10-14_04-17-51',
    current_checkpoint_index=0,
  >
  Check that all nodes in the cluster have read/write access to the configured storage path. `RunConfig(storage_path)` should be set to a cloud storage URI or a shared filesystem path accessible by all nodes in your cluster ('s3://bucket' or '/mnt/nfs'). A local path on the head node is not accessible by worker nodes. See: https://docs.ray.io/en/latest/train/user-guides/persistent-storage.html
  
  Training errored after 10 iterations at 2025-10-14 04:25:29. Total running time: 7min 37s
  Error file: /tmp/ray/session_2025-10-14_04-16-24_757462_1/artifacts/2025-10-14_04-17-48/xgboost_benchmark/driver_artifacts/XGBoostTrainer_6a896_00000_0_2025-10-14_04-17-51/error.txt
  2025-10-14 04:25:33,969 INFO tune.py:1009 -- Wrote the latest version of all result files and experiment state to 'traineddata/cluster_storage_adlfs/xgboost_benchmark' in 4.8581s.
  
  2025-10-14 04:25:33,971 ERROR tune.py:1037 -- Trials did not complete: [XGBoostTrainer_6a896_00000]
  ray.exceptions.RayTaskError(RuntimeError): ray::_Inner.train() (pid=2249, ip=10.244.3.249, actor_id=1450a34748e2f78966d9891502000000, repr=XGBoostTrainer)
    File "/home/ray/anaconda3/lib/python3.10/site-packages/ray/tune/trainable/trainable.py", line 331, in train
      raise skipped from exception_cause(skipped)
    File "/home/ray/anaconda3/lib/python3.10/site-packages/ray/train/_internal/utils.py", line 57, in check_for_failure
      ray.get(object_ref)
  ray.exceptions.RayTaskError(RuntimeError): ray::_RayTrainWorker__execute.get_next() (pid=2327, ip=10.244.3.249, actor_id=f96bf06ce6e268b5342e5a9702000000, repr=<ray.train._internal.worker_group.RayTrainWorker object at 0x7efc4f0ed2a0>)
    File "/home/ray/anaconda3/lib/python3.10/site-packages/ray/train/_internal/worker_group.py", line 33, in __execute
      raise skipped from exception_cause(skipped)
    File "/home/ray/anaconda3/lib/python3.10/site-packages/ray/train/_internal/utils.py", line 176, in discard_return_wrapper
      train_func(*args, **kwargs)
    File "/tmp/ray/session_2025-10-14_04-16-24_757462_1/runtime_resources/working_dir_files/https_github_com_mittachaitu_aks-ray-sample_archive_sai_wi/sample-tuning-setup/direct-blob-access/train_batch_inference_adlfs.py", line 174, in xgboost_train_loop_function
      xgb.train(
    File "/home/ray/anaconda3/lib/python3.10/site-packages/xgboost/core.py", line 726, in inner_f
      return func(**kwargs)
    File "/home/ray/anaconda3/lib/python3.10/site-packages/xgboost/training.py", line 185, in train
      bst = cb_container.after_training(bst)
    File "/home/ray/anaconda3/lib/python3.10/site-packages/xgboost/callback.py", line 190, in after_training
      model = c.after_training(model=model)
    File "/home/ray/anaconda3/lib/python3.10/site-packages/ray/train/xgboost/_xgboost_utils.py", line 153, in after_training
      self._save_and_report_checkpoint(report_dict, model)
    File "/home/ray/anaconda3/lib/python3.10/site-packages/ray/train/xgboost/_xgboost_utils.py", line 249, in _save_and_report_checkpoint
      ray.train.report(report_dict, checkpoint=checkpoint)
    File "/home/ray/anaconda3/lib/python3.10/site-packages/ray/train/_internal/session.py", line 683, in wrapper
      return fn(*args, **kwargs)
    File "/home/ray/anaconda3/lib/python3.10/site-packages/ray/train/_internal/session.py", line 801, in report
      get_session().report(metrics, checkpoint=checkpoint)
    File "/home/ray/anaconda3/lib/python3.10/site-packages/ray/train/_internal/session.py", line 449, in report
      persisted_checkpoint = self.storage.persist_current_checkpoint(checkpoint)
    File "/home/ray/anaconda3/lib/python3.10/site-packages/ray/train/_internal/storage.py", line 547, in persist_current_checkpoint
      self._check_validation_file()
    File "/home/ray/anaconda3/lib/python3.10/site-packages/ray/train/_internal/storage.py", line 499, in _check_validation_file
      raise RuntimeError(
  RuntimeError: Unable to set up cluster storage with the following settings:
  StorageContext<
    storage_filesystem='abfs',
    storage_fs_path='traineddata/cluster_storage_adlfs',
    experiment_dir_name='xgboost_benchmark',
    trial_dir_name='XGBoostTrainer_6a896_00000_0_2025-10-14_04-17-51',
    current_checkpoint_index=0,
  >
  Check that all nodes in the cluster have read/write access to the configured storage path. `RunConfig(storage_path)` should be set to a cloud storage URI or a shared filesystem path accessible by all nodes in your cluster ('s3://bucket' or '/mnt/nfs'). A local path on the head node is not accessible by worker nodes. See: https://docs.ray.io/en/latest/train/user-guides/persistent-storage.html
  
  The above exception was the direct cause of the following exception:
  
  Traceback (most recent call last):
    File "/tmp/ray/session_2025-10-14_04-16-24_757462_1/runtime_resources/working_dir_files/https_github_com_mittachaitu_aks-ray-sample_archive_sai_wi/sample-tuning-setup/direct-blob-access/train_batch_inference_adlfs.py", line 401, in <module>
      main()
    File "/tmp/ray/session_2025-10-14_04-16-24_757462_1/runtime_resources/working_dir_files/https_github_com_mittachaitu_aks-ray-sample_archive_sai_wi/sample-tuning-setup/direct-blob-access/train_batch_inference_adlfs.py", line 372, in main
      result = train(args.framework, data_path, num_workers, cpus_per_worker, args.account_name)
    File "/tmp/ray/session_2025-10-14_04-16-24_757462_1/runtime_resources/working_dir_files/https_github_com_mittachaitu_aks-ray-sample_archive_sai_wi/sample-tuning-setup/direct-blob-access/train_batch_inference_adlfs.py", line 279, in train
      result = trainer.fit()
    File "/home/ray/anaconda3/lib/python3.10/site-packages/ray/train/base_trainer.py", line 722, in fit
      raise TrainingFailedError(
  ray.train.base_trainer.TrainingFailedError: The Ray Train run failed. Please inspect the previous error messages for a cause. After fixing the issue (assuming that the error is not caused by your own application logic, but rather an error such as OOM), you can restart the run from scratch or continue this run.
  To continue this run, you can use: `trainer = XGBoostTrainer.restore("traineddata/cluster_storage_adlfs/xgboost_benchmark")`.
  To start a new run that will retry on training failures, set `train.RunConfig(failure_config=train.FailureConfig(max_failures))` in the Trainer's `run_config` with `max_failures > 0`, or `max_failures = -1` for unlimited retries.
  (RayTrainWorker pid=337, ip=10.244.1.122) [04:17:57] Task [xgboost.ray-rank=00000001]:58d643464f9f40d2d99f50d802000000 got rank 1 [repeated 3x across cluster] (Ray deduplicates logs by default. Set RAY_DEDUP_LOGS=0 to disable log deduplication, or see https://docs.ray.io/en/master/ray-observability/user-guides/configure-logging.html#log-deduplication for more options.)
  2025-10-14 04:25:43,997 ERR cli.py:73 -- -----------------------------------
  2025-10-14 04:25:43,997 ERR cli.py:74 -- Job 'rayjob-tune-gpt2-gpkg7' failed
  2025-10-14 04:25:43,997 ERR cli.py:75 -- -----------------------------------
  2025-10-14 04:25:43,997 INFO cli.py:88 -- Status message: Job entrypoint command failed with exit code 1, last available logs (truncated to 20,000 chars):
    File "/tmp/ray/session_2025-10-14_04-16-24_757462_1/runtime_resources/working_dir_files/https_github_com_mittachaitu_aks-ray-sample_archive_sai_wi/sample-tuning-setup/direct-blob-access/train_batch_inference_adlfs.py", line 372, in main
      result = train(args.framework, data_path, num_workers, cpus_per_worker, args.account_name)
    File "/tmp/ray/session_2025-10-14_04-16-24_757462_1/runtime_resources/working_dir_files/https_github_com_mittachaitu_aks-ray-sample_archive_sai_wi/sample-tuning-setup/direct-blob-access/train_batch_inference_adlfs.py", line 279, in train
      result = trainer.fit()
    File "/home/ray/anaconda3/lib/python3.10/site-packages/ray/train/base_trainer.py", line 722, in fit
      raise TrainingFailedError(
  ray.train.base_trainer.TrainingFailedError: The Ray Train run failed. Please inspect the previous error messages for a cause. After fixing the issue (assuming that the error is not caused by your own application logic, but rather an error such as OOM), you can restart the run from scratch or continue this run.
  To continue this run, you can use: `trainer = XGBoostTrainer.restore("traineddata/cluster_storage_adlfs/xgboost_benchmark")`.
  To start a new run that will retry on training failures, set `train.RunConfig(failure_config=train.FailureConfig(max_failures))` in the Trainer's `run_config` with `max_failures > 0`, or `max_failures = -1` for unlimited retries.
  (RayTrainWorker pid=337, ip=10.244.1.122) [04:17:57] Task [xgboost.ray-rank=00000001]:58d643464f9f40d2d99f50d802000000 got rank 1 [repeated 3x across cluster] (Ray deduplicates logs by default. Set RAY_DEDUP_LOGS=0 to disable log deduplication, or see https://docs.ray.io/en/master/ray-observability/user-guides/configure-logging.html#log-deduplication for more options.)
```
