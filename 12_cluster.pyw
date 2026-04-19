'''
- To see all your data files after execution, run the below command
```shell
%%sh
ls -ltr /data/
```
'''

# Spark Session
from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .appName("Cluster Execution")
    .getOrCreate()
)

df = spark.range(10)

df.write.format("csv").option("header", True).save("/data/output/15/6/range.csv")

'''
# su
root@7cd61605f34d:/home/jupyter/pyspark_notes_and_codes# ./bin/spark-submit --master spark://016e64429d30:7077 --num-executors 3 --executor-cores 2 --executor-memory 512M 12_ckuster.py
bash: ./bin/spark-submit: No such file or directory
root@7cd61605f34d:/home/jupyter/pyspark_notes_and_codes# cd ..
root@7cd61605f34d:/home/jupyter# cd ..
root@7cd61605f34d:/home# cd ..
root@7cd61605f34d:/# cd spark
root@7cd61605f34d:/spark# ./bin/spark-submit --master spark://016e64429d30:7077 --num-executors 3 --executor-cores 2 --executor-memory 512M /home/jupyter/pyspark_notes_and_codes/12_cluster.py
26/04/04 19:21:25 INFO SparkContext: Running Spark version 3.3.0
26/04/04 19:21:25 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
26/04/04 19:21:25 INFO ResourceUtils: ==============================================================
26/04/04 19:21:25 INFO ResourceUtils: No custom resources configured for spark.driver.
26/04/04 19:21:25 INFO ResourceUtils: ==============================================================
26/04/04 19:21:26 INFO SparkContext: Submitted application: Cluster Execution
26/04/04 19:21:26 INFO ResourceProfile: Default ResourceProfile created, executor resources: Map(cores -> name: cores, amount: 2, script: , vendor: , memory -> name: memory, amount: 512, script: , vendor: , offHeap -> name: offHeap, amount: 0, script: , vendor: ), task resources: Map(cpus -> name: cpus, amount: 1.0)
26/04/04 19:21:26 INFO ResourceProfile: Limiting resource is cpus at 2 tasks per executor
26/04/04 19:21:26 INFO ResourceProfileManager: Added ResourceProfile id: 0
26/04/04 19:21:26 INFO SecurityManager: Changing view acls to: root
26/04/04 19:21:26 INFO SecurityManager: Changing modify acls to: root
26/04/04 19:21:26 INFO SecurityManager: Changing view acls groups to: 
26/04/04 19:21:26 INFO SecurityManager: Changing modify acls groups to: 
26/04/04 19:21:26 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(root); groups with view permissions: Set(); users  with modify permissions: Set(root); groups with modify permissions: Set()
26/04/04 19:21:26 INFO Utils: Successfully started service 'sparkDriver' on port 43697.
26/04/04 19:21:27 INFO SparkEnv: Registering MapOutputTracker
26/04/04 19:21:27 INFO SparkEnv: Registering BlockManagerMaster
26/04/04 19:21:27 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
26/04/04 19:21:27 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
26/04/04 19:21:27 INFO SparkEnv: Registering BlockManagerMasterHeartbeat
26/04/04 19:21:27 INFO DiskBlockManager: Created local directory at /tmp/blockmgr-f1f0c2c6-1c10-481a-867e-505d9a1e70e5
26/04/04 19:21:27 INFO MemoryStore: MemoryStore started with capacity 434.4 MiB
26/04/04 19:21:27 INFO SparkEnv: Registering OutputCommitCoordinator
26/04/04 19:21:27 INFO Utils: Successfully started service 'SparkUI' on port 4040.
26/04/04 19:21:28 INFO StandaloneAppClient$ClientEndpoint: Connecting to master spark://016e64429d30:7077...
26/04/04 19:21:28 INFO TransportClientFactory: Successfully created connection to 016e64429d30/172.18.0.2:7077 after 55 ms (0 ms spent in bootstraps)
26/04/04 19:21:28 INFO StandaloneSchedulerBackend: Connected to Spark cluster with app ID app-20260404192128-0003
26/04/04 19:21:28 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 42649.
26/04/04 19:21:28 INFO NettyBlockTransferService: Server created on 7cd61605f34d:42649
26/04/04 19:21:28 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
26/04/04 19:21:28 INFO StandaloneAppClient$ClientEndpoint: Executor added: app-20260404192128-0003/0 on worker-20260404175542-172.18.0.4-40893 (172.18.0.4:40893) with 2 core(s)
26/04/04 19:21:28 INFO StandaloneSchedulerBackend: Granted executor ID app-20260404192128-0003/0 on hostPort 172.18.0.4:40893 with 2 core(s), 512.0 MiB RAM
26/04/04 19:21:28 INFO StandaloneAppClient$ClientEndpoint: Executor added: app-20260404192128-0003/1 on worker-20260404175542-172.18.0.4-40893 (172.18.0.4:40893) with 2 core(s)
26/04/04 19:21:28 INFO StandaloneSchedulerBackend: Granted executor ID app-20260404192128-0003/1 on hostPort 172.18.0.4:40893 with 2 core(s), 512.0 MiB RAM
26/04/04 19:21:28 INFO StandaloneAppClient$ClientEndpoint: Executor added: app-20260404192128-0003/2 on worker-20260404175542-172.18.0.4-40893 (172.18.0.4:40893) with 2 core(s)
26/04/04 19:21:28 INFO StandaloneSchedulerBackend: Granted executor ID app-20260404192128-0003/2 on hostPort 172.18.0.4:40893 with 2 core(s), 512.0 MiB RAM
26/04/04 19:21:28 INFO StandaloneAppClient$ClientEndpoint: Executor added: app-20260404192128-0003/3 on worker-20260404175542-172.18.0.4-40893 (172.18.0.4:40893) with 2 core(s)
26/04/04 19:21:28 INFO StandaloneSchedulerBackend: Granted executor ID app-20260404192128-0003/3 on hostPort 172.18.0.4:40893 with 2 core(s), 512.0 MiB RAM
26/04/04 19:21:28 INFO StandaloneAppClient$ClientEndpoint: Executor added: app-20260404192128-0003/4 on worker-20260404175542-172.18.0.5-38679 (172.18.0.5:38679) with 2 core(s)
26/04/04 19:21:28 INFO StandaloneSchedulerBackend: Granted executor ID app-20260404192128-0003/4 on hostPort 172.18.0.5:38679 with 2 core(s), 512.0 MiB RAM
26/04/04 19:21:28 INFO StandaloneAppClient$ClientEndpoint: Executor added: app-20260404192128-0003/5 on worker-20260404175542-172.18.0.5-38679 (172.18.0.5:38679) with 2 core(s)
26/04/04 19:21:28 INFO StandaloneSchedulerBackend: Granted executor ID app-20260404192128-0003/5 on hostPort 172.18.0.5:38679 with 2 core(s), 512.0 MiB RAM
26/04/04 19:21:28 INFO StandaloneAppClient$ClientEndpoint: Executor added: app-20260404192128-0003/6 on worker-20260404175542-172.18.0.5-38679 (172.18.0.5:38679) with 2 core(s)
26/04/04 19:21:28 INFO StandaloneSchedulerBackend: Granted executor ID app-20260404192128-0003/6 on hostPort 172.18.0.5:38679 with 2 core(s), 512.0 MiB RAM
26/04/04 19:21:28 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 7cd61605f34d, 42649, None)
26/04/04 19:21:28 INFO StandaloneAppClient$ClientEndpoint: Executor added: app-20260404192128-0003/7 on worker-20260404175542-172.18.0.5-38679 (172.18.0.5:38679) with 2 core(s)
26/04/04 19:21:28 INFO StandaloneSchedulerBackend: Granted executor ID app-20260404192128-0003/7 on hostPort 172.18.0.5:38679 with 2 core(s), 512.0 MiB RAM
26/04/04 19:21:28 INFO BlockManagerMasterEndpoint: Registering block manager 7cd61605f34d:42649 with 434.4 MiB RAM, BlockManagerId(driver, 7cd61605f34d, 42649, None)
26/04/04 19:21:28 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 7cd61605f34d, 42649, None)
26/04/04 19:21:28 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 7cd61605f34d, 42649, None)
26/04/04 19:21:29 INFO StandaloneAppClient$ClientEndpoint: Executor updated: app-20260404192128-0003/2 is now RUNNING
26/04/04 19:21:29 INFO StandaloneAppClient$ClientEndpoint: Executor updated: app-20260404192128-0003/0 is now RUNNING
26/04/04 19:21:29 INFO StandaloneAppClient$ClientEndpoint: Executor updated: app-20260404192128-0003/6 is now RUNNING
26/04/04 19:21:29 INFO StandaloneAppClient$ClientEndpoint: Executor updated: app-20260404192128-0003/1 is now RUNNING
26/04/04 19:21:29 INFO StandaloneAppClient$ClientEndpoint: Executor updated: app-20260404192128-0003/3 is now RUNNING
26/04/04 19:21:29 INFO StandaloneAppClient$ClientEndpoint: Executor updated: app-20260404192128-0003/7 is now RUNNING
26/04/04 19:21:29 INFO StandaloneAppClient$ClientEndpoint: Executor updated: app-20260404192128-0003/5 is now RUNNING
26/04/04 19:21:29 INFO StandaloneAppClient$ClientEndpoint: Executor updated: app-20260404192128-0003/4 is now RUNNING
26/04/04 19:21:30 INFO StandaloneSchedulerBackend: SchedulerBackend is ready for scheduling beginning after reached minRegisteredResourcesRatio: 0.0
26/04/04 19:21:38 INFO SharedState: Setting hive.metastore.warehouse.dir ('null') to the value of spark.sql.warehouse.dir.
26/04/04 19:21:38 INFO SharedState: Warehouse path is 'file:/spark/spark-warehouse'.
26/04/04 19:21:47 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
26/04/04 19:21:47 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
26/04/04 19:21:47 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
26/04/04 19:21:56 INFO CodeGenerator: Code generated in 2533.313954 ms
26/04/04 19:22:04 INFO SparkContext: Starting job: save at NativeMethodAccessorImpl.java:0
26/04/04 19:22:04 INFO DAGScheduler: Got job 0 (save at NativeMethodAccessorImpl.java:0) with 2 output partitions
26/04/04 19:22:04 INFO DAGScheduler: Final stage: ResultStage 0 (save at NativeMethodAccessorImpl.java:0)
26/04/04 19:22:04 INFO DAGScheduler: Parents of final stage: List()
26/04/04 19:22:04 INFO DAGScheduler: Missing parents: List()
26/04/04 19:22:04 INFO DAGScheduler: Submitting ResultStage 0 (MapPartitionsRDD[2] at save at NativeMethodAccessorImpl.java:0), which has no missing parents
26/04/04 19:22:04 INFO CoarseGrainedSchedulerBackend$DriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (172.18.0.5:41180) with ID 6,  ResourceProfileId 0
26/04/04 19:22:04 INFO CoarseGrainedSchedulerBackend$DriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (172.18.0.4:40720) with ID 0,  ResourceProfileId 0
26/04/04 19:22:04 INFO CoarseGrainedSchedulerBackend$DriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (172.18.0.4:40696) with ID 3,  ResourceProfileId 0
26/04/04 19:22:04 INFO CoarseGrainedSchedulerBackend$DriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (172.18.0.4:40686) with ID 1,  ResourceProfileId 0
26/04/04 19:22:04 INFO CoarseGrainedSchedulerBackend$DriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (172.18.0.5:41150) with ID 5,  ResourceProfileId 0
26/04/04 19:22:04 INFO CoarseGrainedSchedulerBackend$DriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (172.18.0.4:40708) with ID 2,  ResourceProfileId 0
26/04/04 19:22:04 INFO CoarseGrainedSchedulerBackend$DriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (172.18.0.5:41168) with ID 7,  ResourceProfileId 0
26/04/04 19:22:04 INFO CoarseGrainedSchedulerBackend$DriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (172.18.0.5:41154) with ID 4,  ResourceProfileId 0
26/04/04 19:22:06 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 211.3 KiB, free 434.2 MiB)
26/04/04 19:22:06 INFO BlockManagerMasterEndpoint: Registering block manager 172.18.0.4:45407 with 93.3 MiB RAM, BlockManagerId(1, 172.18.0.4, 45407, None)
26/04/04 19:22:06 INFO BlockManagerMasterEndpoint: Registering block manager 172.18.0.4:34811 with 93.3 MiB RAM, BlockManagerId(2, 172.18.0.4, 34811, None)
26/04/04 19:22:06 INFO BlockManagerMasterEndpoint: Registering block manager 172.18.0.4:33747 with 93.3 MiB RAM, BlockManagerId(3, 172.18.0.4, 33747, None)
26/04/04 19:22:06 INFO BlockManagerMasterEndpoint: Registering block manager 172.18.0.5:46743 with 93.3 MiB RAM, BlockManagerId(5, 172.18.0.5, 46743, None)
26/04/04 19:22:06 INFO BlockManagerMasterEndpoint: Registering block manager 172.18.0.5:41747 with 93.3 MiB RAM, BlockManagerId(6, 172.18.0.5, 41747, None)
26/04/04 19:22:06 INFO BlockManagerMasterEndpoint: Registering block manager 172.18.0.5:43935 with 93.3 MiB RAM, BlockManagerId(4, 172.18.0.5, 43935, None)
26/04/04 19:22:06 INFO BlockManagerMasterEndpoint: Registering block manager 172.18.0.5:34955 with 93.3 MiB RAM, BlockManagerId(7, 172.18.0.5, 34955, None)
26/04/04 19:22:07 INFO BlockManagerMasterEndpoint: Registering block manager 172.18.0.4:42707 with 93.3 MiB RAM, BlockManagerId(0, 172.18.0.4, 42707, None)
26/04/04 19:22:07 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 76.0 KiB, free 434.1 MiB)
26/04/04 19:22:07 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 7cd61605f34d:42649 (size: 76.0 KiB, free: 434.3 MiB)
26/04/04 19:22:07 INFO SparkContext: Created broadcast 0 from broadcast at DAGScheduler.scala:1513
26/04/04 19:22:07 INFO DAGScheduler: Submitting 2 missing tasks from ResultStage 0 (MapPartitionsRDD[2] at save at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0, 1))
26/04/04 19:22:07 INFO TaskSchedulerImpl: Adding task set 0.0 with 2 tasks resource profile 0
26/04/04 19:22:07 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0) (172.18.0.5, executor 6, partition 0, PROCESS_LOCAL, 4582 bytes) taskResourceAssignments Map()
26/04/04 19:22:07 INFO TaskSetManager: Starting task 1.0 in stage 0.0 (TID 1) (172.18.0.5, executor 6, partition 1, PROCESS_LOCAL, 4582 bytes) taskResourceAssignments Map()
26/04/04 19:22:12 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 172.18.0.5:41747 (size: 76.0 KiB, free: 93.2 MiB)
26/04/04 19:22:14 INFO TaskSetManager: Finished task 1.0 in stage 0.0 (TID 1) in 7001 ms on 172.18.0.5 (executor 6) (1/2)
26/04/04 19:22:14 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 7186 ms on 172.18.0.5 (executor 6) (2/2)
26/04/04 19:22:14 INFO TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool 
26/04/04 19:22:14 INFO DAGScheduler: ResultStage 0 (save at NativeMethodAccessorImpl.java:0) finished in 10.257 s
26/04/04 19:22:14 INFO DAGScheduler: Job 0 is finished. Cancelling potential speculative or zombie tasks for this job
26/04/04 19:22:14 INFO TaskSchedulerImpl: Killing all running tasks in stage 0: Stage finished
26/04/04 19:22:14 INFO DAGScheduler: Job 0 finished: save at NativeMethodAccessorImpl.java:0, took 10.743111 s
26/04/04 19:22:14 INFO FileFormatWriter: Start to commit write Job e3fbe392-d09a-41d2-b7af-bf6d1d956ca9.
26/04/04 19:22:15 INFO FileFormatWriter: Write Job e3fbe392-d09a-41d2-b7af-bf6d1d956ca9 committed. Elapsed time: 145 ms.
26/04/04 19:22:15 INFO FileFormatWriter: Finished processing stats for write job e3fbe392-d09a-41d2-b7af-bf6d1d956ca9.
26/04/04 19:22:15 INFO SparkContext: Invoking stop() from shutdown hook
26/04/04 19:22:15 INFO SparkUI: Stopped Spark web UI at http://7cd61605f34d:4040
26/04/04 19:22:15 INFO StandaloneSchedulerBackend: Shutting down all executors
26/04/04 19:22:15 INFO CoarseGrainedSchedulerBackend$DriverEndpoint: Asking each executor to shut down
26/04/04 19:22:15 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
26/04/04 19:22:15 INFO MemoryStore: MemoryStore cleared
26/04/04 19:22:15 INFO BlockManager: BlockManager stopped
26/04/04 19:22:15 INFO BlockManagerMaster: BlockManagerMaster stopped
26/04/04 19:22:15 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
26/04/04 19:22:15 INFO SparkContext: Successfully stopped SparkContext
26/04/04 19:22:15 INFO ShutdownHookManager: Shutdown hook called
26/04/04 19:22:15 INFO ShutdownHookManager: Deleting directory /tmp/spark-eaf07635-3825-4bd4-af2c-b951a58a43e9/pyspark-149f0f8a-f2f9-47c5-9694-762d52073e06
26/04/04 19:22:15 INFO ShutdownHookManager: Deleting directory /tmp/spark-c780a6d4-7866-47b5-9590-265df88a1a6e
26/04/04 19:22:15 INFO ShutdownHookManager: Deleting directory /tmp/spark-eaf07635-3825-4bd4-af2c-b951a58a43e9
root@7cd61605f34d:/spark# 
'''