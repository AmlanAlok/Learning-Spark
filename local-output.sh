amlanalok@Amlans-MacBook-Pro Learning-Spark % spark-submit --class Graph target/cse6331-project5-0.1.jar small-graph.csv
22/04/03 15:42:20 WARN Utils: Your hostname, Amlans-MacBook-Pro.local resolves to a loopback address: 127.0.0.1; using 10.219.149.130 instead (on interface en0)
22/04/03 15:42:20 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
WARNING: An illegal reflective access operation has occurred
WARNING: Illegal reflective access by org.apache.spark.unsafe.Platform (file:/opt/homebrew/Cellar/apache-spark/3.2.0/libexec/jars/spark-unsafe_2.12-3.2.0.jar) to constructor java.nio.DirectByteBuffer(long,int)
WARNING: Please consider reporting this to the maintainers of org.apache.spark.unsafe.Platform
WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
WARNING: All illegal access operations will be denied in a future release
Using Spark\'s default log4j profile: org/apache/spark/log4j-defaults.properties
22/04/03 15:42:21 INFO SparkContext: Running Spark version 3.2.0
22/04/03 15:42:21 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
22/04/03 15:42:21 INFO ResourceUtils: ==============================================================
22/04/03 15:42:21 INFO ResourceUtils: No custom resources configured for spark.driver.
22/04/03 15:42:21 INFO ResourceUtils: ==============================================================
22/04/03 15:42:21 INFO SparkContext: Submitted application: Graph
22/04/03 15:42:21 INFO ResourceProfile: Default ResourceProfile created, executor resources: Map(cores -> name: cores, amount: 1, script: , vendor: , memory -> name: memory, amount: 1024, script: , vendor: , offHeap -> name: offHeap, amount: 0, script: , vendor: ), task resources: Map(cpus -> name: cpus, amount: 1.0)
22/04/03 15:42:21 INFO ResourceProfile: Limiting resource is cpu
22/04/03 15:42:21 INFO ResourceProfileManager: Added ResourceProfile id: 0
22/04/03 15:42:21 INFO SecurityManager: Changing view acls to: amlanalok
22/04/03 15:42:21 INFO SecurityManager: Changing modify acls to: amlanalok
22/04/03 15:42:21 INFO SecurityManager: Changing view acls groups to:
22/04/03 15:42:21 INFO SecurityManager: Changing modify acls groups to:
22/04/03 15:42:21 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(amlanalok); groups with view permissions: Set(); users  with modify permissions: Set(amlanalok); groups with modify permissions: Set()
22/04/03 15:42:21 INFO Utils: Successfully started service 'sparkDriver' on port 50045.
22/04/03 15:42:21 INFO SparkEnv: Registering MapOutputTracker
22/04/03 15:42:21 INFO SparkEnv: Registering BlockManagerMaster
22/04/03 15:42:21 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
22/04/03 15:42:21 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
22/04/03 15:42:21 INFO SparkEnv: Registering BlockManagerMasterHeartbeat
22/04/03 15:42:21 INFO DiskBlockManager: Created local directory at /private/var/folders/x8/lld34qc52sj2p7ssk9ylpp5r0000gn/T/blockmgr-4081b776-af70-46c3-a928-a45cdb74cf3a
22/04/03 15:42:21 INFO MemoryStore: MemoryStore started with capacity 434.4 MiB
22/04/03 15:42:21 INFO SparkEnv: Registering OutputCommitCoordinator
22/04/03 15:42:21 INFO Utils: Successfully started service 'SparkUI' on port 4040.
22/04/03 15:42:21 INFO SparkUI: Bound SparkUI to 0.0.0.0, and started at http://10.219.149.130:4040
22/04/03 15:42:21 INFO SparkContext: Added JAR file:/Users/amlanalok/Documents/UTA/MS-CS/3-Spring-2022/2-CSE-6331-Advanced%20Database/1-All-Projects/5-Project-5/Code/Learning-Spark/target/cse6331-project5-0.1.jar at spark://10.219.149.130:50045/jars/cse6331-project5-0.1.jar with timestamp 1649018541053
22/04/03 15:42:21 INFO Executor: Starting executor ID driver on host 10.219.149.130
22/04/03 15:42:21 INFO Executor: Fetching spark://10.219.149.130:50045/jars/cse6331-project5-0.1.jar with timestamp 1649018541053
22/04/03 15:42:21 INFO TransportClientFactory: Successfully created connection to /10.219.149.130:50045 after 15 ms (0 ms spent in bootstraps)
22/04/03 15:42:21 INFO Utils: Fetching spark://10.219.149.130:50045/jars/cse6331-project5-0.1.jar to /private/var/folders/x8/lld34qc52sj2p7ssk9ylpp5r0000gn/T/spark-5e91e666-98cd-45d8-b081-5966ad1461e9/userFiles-b1d574aa-cbb1-48d9-8f0f-74821bc85694/fetchFileTemp3665827964204666253.tmp
22/04/03 15:42:21 INFO Executor: Adding file:/private/var/folders/x8/lld34qc52sj2p7ssk9ylpp5r0000gn/T/spark-5e91e666-98cd-45d8-b081-5966ad1461e9/userFiles-b1d574aa-cbb1-48d9-8f0f-74821bc85694/cse6331-project5-0.1.jar to class loader
22/04/03 15:42:21 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 50047.
22/04/03 15:42:21 INFO NettyBlockTransferService: Server created on 10.219.149.130:50047
22/04/03 15:42:21 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
22/04/03 15:42:21 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 10.219.149.130, 50047, None)
22/04/03 15:42:21 INFO BlockManagerMasterEndpoint: Registering block manager 10.219.149.130:50047 with 434.4 MiB RAM, BlockManagerId(driver, 10.219.149.130, 50047, None)
22/04/03 15:42:21 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 10.219.149.130, 50047, None)
22/04/03 15:42:21 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 10.219.149.130, 50047, None)
22/04/03 15:42:22 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 219.7 KiB, free 434.2 MiB)
22/04/03 15:42:22 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 32.1 KiB, free 434.2 MiB)
22/04/03 15:42:22 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 10.219.149.130:50047 (size: 32.1 KiB, free: 434.4 MiB)
22/04/03 15:42:22 INFO SparkContext: Created broadcast 0 from textFile at Graph.scala:136
22/04/03 15:42:22 INFO FileInputFormat: Total input files to process : 1
kvPairs
22/04/03 15:42:22 INFO SparkContext: Starting job: collect at Graph.scala:140
22/04/03 15:42:22 INFO DAGScheduler: Registering RDD 2 (map at Graph.scala:138) as input to shuffle 0
22/04/03 15:42:22 INFO DAGScheduler: Got job 0 (collect at Graph.scala:140) with 2 output partitions
22/04/03 15:42:22 INFO DAGScheduler: Final stage: ResultStage 1 (collect at Graph.scala:140)
22/04/03 15:42:22 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 0)
22/04/03 15:42:22 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 0)
22/04/03 15:42:22 INFO DAGScheduler: Submitting ShuffleMapStage 0 (MapPartitionsRDD[2] at map at Graph.scala:138), which has no missing parents
22/04/03 15:42:22 INFO MemoryStore: Block broadcast_1 stored as values in memory (estimated size 7.4 KiB, free 434.1 MiB)
22/04/03 15:42:22 INFO MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 4.1 KiB, free 434.1 MiB)
22/04/03 15:42:22 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on 10.219.149.130:50047 (size: 4.1 KiB, free: 434.4 MiB)
22/04/03 15:42:22 INFO SparkContext: Created broadcast 1 from broadcast at DAGScheduler.scala:1427
22/04/03 15:42:22 INFO DAGScheduler: Submitting 2 missing tasks from ShuffleMapStage 0 (MapPartitionsRDD[2] at map at Graph.scala:138) (first 15 tasks are for partitions Vector(0, 1))
22/04/03 15:42:22 INFO TaskSchedulerImpl: Adding task set 0.0 with 2 tasks resource profile 0
22/04/03 15:42:22 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0) (10.219.149.130, executor driver, partition 0, PROCESS_LOCAL, 4606 bytes) taskResourceAssignments Map()
22/04/03 15:42:22 INFO TaskSetManager: Starting task 1.0 in stage 0.0 (TID 1) (10.219.149.130, executor driver, partition 1, PROCESS_LOCAL, 4606 bytes) taskResourceAssignments Map()
22/04/03 15:42:22 INFO Executor: Running task 0.0 in stage 0.0 (TID 0)
22/04/03 15:42:22 INFO Executor: Running task 1.0 in stage 0.0 (TID 1)
22/04/03 15:42:22 INFO HadoopRDD: Input split: file:/Users/amlanalok/Documents/UTA/MS-CS/3-Spring-2022/2-CSE-6331-Advanced Database/1-All-Projects/5-Project-5/Code/Learning-Spark/small-graph.csv:18+18
22/04/03 15:42:22 INFO HadoopRDD: Input split: file:/Users/amlanalok/Documents/UTA/MS-CS/3-Spring-2022/2-CSE-6331-Advanced Database/1-All-Projects/5-Project-5/Code/Learning-Spark/small-graph.csv:0+18
22/04/03 15:42:22 INFO Executor: Finished task 1.0 in stage 0.0 (TID 1). 1164 bytes result sent to driver
22/04/03 15:42:22 INFO Executor: Finished task 0.0 in stage 0.0 (TID 0). 1164 bytes result sent to driver
22/04/03 15:42:22 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 279 ms on 10.219.149.130 (executor driver) (1/2)
22/04/03 15:42:22 INFO TaskSetManager: Finished task 1.0 in stage 0.0 (TID 1) in 270 ms on 10.219.149.130 (executor driver) (2/2)
22/04/03 15:42:22 INFO TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool
22/04/03 15:42:22 INFO DAGScheduler: ShuffleMapStage 0 (map at Graph.scala:138) finished in 0.344 s
22/04/03 15:42:22 INFO DAGScheduler: looking for newly runnable stages
22/04/03 15:42:22 INFO DAGScheduler: running: Set()
22/04/03 15:42:22 INFO DAGScheduler: waiting: Set(ResultStage 1)
22/04/03 15:42:22 INFO DAGScheduler: failed: Set()
22/04/03 15:42:22 INFO DAGScheduler: Submitting ResultStage 1 (ShuffledRDD[3] at groupByKey at Graph.scala:138), which has no missing parents
22/04/03 15:42:22 INFO MemoryStore: Block broadcast_2 stored as values in memory (estimated size 8.3 KiB, free 434.1 MiB)
22/04/03 15:42:22 INFO MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 4.5 KiB, free 434.1 MiB)
22/04/03 15:42:22 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on 10.219.149.130:50047 (size: 4.5 KiB, free: 434.4 MiB)
22/04/03 15:42:22 INFO SparkContext: Created broadcast 2 from broadcast at DAGScheduler.scala:1427
22/04/03 15:42:22 INFO DAGScheduler: Submitting 2 missing tasks from ResultStage 1 (ShuffledRDD[3] at groupByKey at Graph.scala:138) (first 15 tasks are for partitions Vector(0, 1))
22/04/03 15:42:22 INFO TaskSchedulerImpl: Adding task set 1.0 with 2 tasks resource profile 0
22/04/03 15:42:22 INFO TaskSetManager: Starting task 0.0 in stage 1.0 (TID 2) (10.219.149.130, executor driver, partition 0, NODE_LOCAL, 4271 bytes) taskResourceAssignments Map()
22/04/03 15:42:22 INFO TaskSetManager: Starting task 1.0 in stage 1.0 (TID 3) (10.219.149.130, executor driver, partition 1, NODE_LOCAL, 4271 bytes) taskResourceAssignments Map()
22/04/03 15:42:22 INFO Executor: Running task 0.0 in stage 1.0 (TID 2)
22/04/03 15:42:22 INFO Executor: Running task 1.0 in stage 1.0 (TID 3)
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Getting 2 (108.0 B) non-empty blocks including 2 (108.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Getting 2 (103.0 B) non-empty blocks including 2 (103.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 8 ms
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 8 ms
22/04/03 15:42:22 INFO Executor: Finished task 1.0 in stage 1.0 (TID 3). 1904 bytes result sent to driver
22/04/03 15:42:22 INFO Executor: Finished task 0.0 in stage 1.0 (TID 2). 1824 bytes result sent to driver
22/04/03 15:42:22 INFO TaskSetManager: Finished task 1.0 in stage 1.0 (TID 3) in 42 ms on 10.219.149.130 (executor driver) (1/2)
22/04/03 15:42:22 INFO TaskSetManager: Finished task 0.0 in stage 1.0 (TID 2) in 43 ms on 10.219.149.130 (executor driver) (2/2)
22/04/03 15:42:22 INFO TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool
22/04/03 15:42:22 INFO DAGScheduler: ResultStage 1 (collect at Graph.scala:140) finished in 0.050 s
22/04/03 15:42:22 INFO DAGScheduler: Job 0 is finished. Cancelling potential speculative or zombie tasks for this job
22/04/03 15:42:22 INFO TaskSchedulerImpl: Killing all running tasks in stage 1: Stage finished
22/04/03 15:42:22 INFO DAGScheduler: Job 0 finished: collect at Graph.scala:140, took 0.431168 s
(4,CompactBuffer(6))
(2,CompactBuffer(3, 4))
(1,CompactBuffer(2, 3))
(3,CompactBuffer(4, 5))
(7,CompactBuffer(3))
(5,CompactBuffer(1))
22/04/03 15:42:22 INFO SparkContext: Starting job: sortByKey at Graph.scala:144
22/04/03 15:42:22 INFO DAGScheduler: Got job 1 (sortByKey at Graph.scala:144) with 2 output partitions
22/04/03 15:42:22 INFO DAGScheduler: Final stage: ResultStage 3 (sortByKey at Graph.scala:144)
22/04/03 15:42:22 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 2)
22/04/03 15:42:22 INFO DAGScheduler: Missing parents: List()
22/04/03 15:42:22 INFO DAGScheduler: Submitting ResultStage 3 (MapPartitionsRDD[6] at sortByKey at Graph.scala:144), which has no missing parents
22/04/03 15:42:22 INFO MemoryStore: Block broadcast_3 stored as values in memory (estimated size 9.7 KiB, free 434.1 MiB)
22/04/03 15:42:22 INFO MemoryStore: Block broadcast_3_piece0 stored as bytes in memory (estimated size 4.9 KiB, free 434.1 MiB)
22/04/03 15:42:22 INFO BlockManagerInfo: Added broadcast_3_piece0 in memory on 10.219.149.130:50047 (size: 4.9 KiB, free: 434.4 MiB)
22/04/03 15:42:22 INFO SparkContext: Created broadcast 3 from broadcast at DAGScheduler.scala:1427
22/04/03 15:42:22 INFO DAGScheduler: Submitting 2 missing tasks from ResultStage 3 (MapPartitionsRDD[6] at sortByKey at Graph.scala:144) (first 15 tasks are for partitions Vector(0, 1))
22/04/03 15:42:22 INFO TaskSchedulerImpl: Adding task set 3.0 with 2 tasks resource profile 0
22/04/03 15:42:22 INFO TaskSetManager: Starting task 0.0 in stage 3.0 (TID 4) (10.219.149.130, executor driver, partition 0, NODE_LOCAL, 4271 bytes) taskResourceAssignments Map()
22/04/03 15:42:22 INFO TaskSetManager: Starting task 1.0 in stage 3.0 (TID 5) (10.219.149.130, executor driver, partition 1, NODE_LOCAL, 4271 bytes) taskResourceAssignments Map()
22/04/03 15:42:22 INFO Executor: Running task 1.0 in stage 3.0 (TID 5)
22/04/03 15:42:22 INFO Executor: Running task 0.0 in stage 3.0 (TID 4)
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Getting 2 (103.0 B) non-empty blocks including 2 (103.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Getting 2 (108.0 B) non-empty blocks including 2 (108.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:22 INFO Executor: Finished task 0.0 in stage 3.0 (TID 4). 1500 bytes result sent to driver
22/04/03 15:42:22 INFO Executor: Finished task 1.0 in stage 3.0 (TID 5). 1508 bytes result sent to driver
22/04/03 15:42:22 INFO TaskSetManager: Finished task 0.0 in stage 3.0 (TID 4) in 26 ms on 10.219.149.130 (executor driver) (1/2)
22/04/03 15:42:22 INFO TaskSetManager: Finished task 1.0 in stage 3.0 (TID 5) in 25 ms on 10.219.149.130 (executor driver) (2/2)
22/04/03 15:42:22 INFO TaskSchedulerImpl: Removed TaskSet 3.0, whose tasks have all completed, from pool
22/04/03 15:42:22 INFO DAGScheduler: ResultStage 3 (sortByKey at Graph.scala:144) finished in 0.033 s
22/04/03 15:42:22 INFO DAGScheduler: Job 1 is finished. Cancelling potential speculative or zombie tasks for this job
22/04/03 15:42:22 INFO TaskSchedulerImpl: Killing all running tasks in stage 3: Stage finished
22/04/03 15:42:22 INFO DAGScheduler: Job 1 finished: sortByKey at Graph.scala:144, took 0.035371 s
followerList
22/04/03 15:42:22 INFO SparkContext: Starting job: collect at Graph.scala:146
22/04/03 15:42:22 INFO DAGScheduler: Registering RDD 4 (map at Graph.scala:144) as input to shuffle 1
22/04/03 15:42:22 INFO DAGScheduler: Got job 2 (collect at Graph.scala:146) with 2 output partitions
22/04/03 15:42:22 INFO DAGScheduler: Final stage: ResultStage 6 (collect at Graph.scala:146)
22/04/03 15:42:22 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 5)
22/04/03 15:42:22 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 5)
22/04/03 15:42:22 INFO DAGScheduler: Submitting ShuffleMapStage 5 (MapPartitionsRDD[4] at map at Graph.scala:144), which has no missing parents
22/04/03 15:42:22 INFO MemoryStore: Block broadcast_4 stored as values in memory (estimated size 8.6 KiB, free 434.1 MiB)
22/04/03 15:42:22 INFO MemoryStore: Block broadcast_4_piece0 stored as bytes in memory (estimated size 4.7 KiB, free 434.1 MiB)
22/04/03 15:42:22 INFO BlockManagerInfo: Added broadcast_4_piece0 in memory on 10.219.149.130:50047 (size: 4.7 KiB, free: 434.4 MiB)
22/04/03 15:42:22 INFO SparkContext: Created broadcast 4 from broadcast at DAGScheduler.scala:1427
22/04/03 15:42:22 INFO DAGScheduler: Submitting 2 missing tasks from ShuffleMapStage 5 (MapPartitionsRDD[4] at map at Graph.scala:144) (first 15 tasks are for partitions Vector(0, 1))
22/04/03 15:42:22 INFO TaskSchedulerImpl: Adding task set 5.0 with 2 tasks resource profile 0
22/04/03 15:42:22 INFO TaskSetManager: Starting task 0.0 in stage 5.0 (TID 6) (10.219.149.130, executor driver, partition 0, NODE_LOCAL, 4260 bytes) taskResourceAssignments Map()
22/04/03 15:42:22 INFO TaskSetManager: Starting task 1.0 in stage 5.0 (TID 7) (10.219.149.130, executor driver, partition 1, NODE_LOCAL, 4260 bytes) taskResourceAssignments Map()
22/04/03 15:42:22 INFO Executor: Running task 1.0 in stage 5.0 (TID 7)
22/04/03 15:42:22 INFO Executor: Running task 0.0 in stage 5.0 (TID 6)
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Getting 2 (108.0 B) non-empty blocks including 2 (108.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Getting 2 (103.0 B) non-empty blocks including 2 (103.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:22 INFO Executor: Finished task 1.0 in stage 5.0 (TID 7). 1465 bytes result sent to driver
22/04/03 15:42:22 INFO Executor: Finished task 0.0 in stage 5.0 (TID 6). 1465 bytes result sent to driver
22/04/03 15:42:22 INFO TaskSetManager: Finished task 1.0 in stage 5.0 (TID 7) in 18 ms on 10.219.149.130 (executor driver) (1/2)
22/04/03 15:42:22 INFO TaskSetManager: Finished task 0.0 in stage 5.0 (TID 6) in 20 ms on 10.219.149.130 (executor driver) (2/2)
22/04/03 15:42:22 INFO TaskSchedulerImpl: Removed TaskSet 5.0, whose tasks have all completed, from pool
22/04/03 15:42:22 INFO DAGScheduler: ShuffleMapStage 5 (map at Graph.scala:144) finished in 0.027 s
22/04/03 15:42:22 INFO DAGScheduler: looking for newly runnable stages
22/04/03 15:42:22 INFO DAGScheduler: running: Set()
22/04/03 15:42:22 INFO DAGScheduler: waiting: Set(ResultStage 6)
22/04/03 15:42:22 INFO DAGScheduler: failed: Set()
22/04/03 15:42:22 INFO DAGScheduler: Submitting ResultStage 6 (ShuffledRDD[7] at sortByKey at Graph.scala:144), which has no missing parents
22/04/03 15:42:22 INFO MemoryStore: Block broadcast_5 stored as values in memory (estimated size 5.1 KiB, free 434.1 MiB)
22/04/03 15:42:22 INFO MemoryStore: Block broadcast_5_piece0 stored as bytes in memory (estimated size 3.0 KiB, free 434.1 MiB)
22/04/03 15:42:22 INFO BlockManagerInfo: Added broadcast_5_piece0 in memory on 10.219.149.130:50047 (size: 3.0 KiB, free: 434.3 MiB)
22/04/03 15:42:22 INFO SparkContext: Created broadcast 5 from broadcast at DAGScheduler.scala:1427
22/04/03 15:42:22 INFO DAGScheduler: Submitting 2 missing tasks from ResultStage 6 (ShuffledRDD[7] at sortByKey at Graph.scala:144) (first 15 tasks are for partitions Vector(0, 1))
22/04/03 15:42:22 INFO TaskSchedulerImpl: Adding task set 6.0 with 2 tasks resource profile 0
22/04/03 15:42:22 INFO TaskSetManager: Starting task 0.0 in stage 6.0 (TID 8) (10.219.149.130, executor driver, partition 0, NODE_LOCAL, 4271 bytes) taskResourceAssignments Map()
22/04/03 15:42:22 INFO TaskSetManager: Starting task 1.0 in stage 6.0 (TID 9) (10.219.149.130, executor driver, partition 1, NODE_LOCAL, 4271 bytes) taskResourceAssignments Map()
22/04/03 15:42:22 INFO Executor: Running task 0.0 in stage 6.0 (TID 8)
22/04/03 15:42:22 INFO Executor: Running task 1.0 in stage 6.0 (TID 9)
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Getting 2 (114.0 B) non-empty blocks including 2 (114.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Getting 2 (108.0 B) non-empty blocks including 2 (108.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:22 INFO Executor: Finished task 0.0 in stage 6.0 (TID 8). 1467 bytes result sent to driver
22/04/03 15:42:22 INFO Executor: Finished task 1.0 in stage 6.0 (TID 9). 1467 bytes result sent to driver
22/04/03 15:42:22 INFO TaskSetManager: Finished task 0.0 in stage 6.0 (TID 8) in 17 ms on 10.219.149.130 (executor driver) (1/2)
22/04/03 15:42:22 INFO TaskSetManager: Finished task 1.0 in stage 6.0 (TID 9) in 17 ms on 10.219.149.130 (executor driver) (2/2)
22/04/03 15:42:22 INFO TaskSchedulerImpl: Removed TaskSet 6.0, whose tasks have all completed, from pool
22/04/03 15:42:22 INFO DAGScheduler: ResultStage 6 (collect at Graph.scala:146) finished in 0.023 s
22/04/03 15:42:22 INFO DAGScheduler: Job 2 is finished. Cancelling potential speculative or zombie tasks for this job
22/04/03 15:42:22 INFO TaskSchedulerImpl: Killing all running tasks in stage 6: Stage finished
22/04/03 15:42:22 INFO DAGScheduler: Job 2 finished: collect at Graph.scala:146, took 0.054570 s
(1,0)
(2,2147483647)
(3,2147483647)
(4,2147483647)
(5,2147483647)
(7,2147483647)
join1
22/04/03 15:42:22 INFO SparkContext: Starting job: collect at Graph.scala:150
22/04/03 15:42:22 INFO DAGScheduler: Registering RDD 3 (groupByKey at Graph.scala:138) as input to shuffle 2
22/04/03 15:42:22 INFO DAGScheduler: Got job 3 (collect at Graph.scala:150) with 2 output partitions
22/04/03 15:42:22 INFO DAGScheduler: Final stage: ResultStage 10 (collect at Graph.scala:150)
22/04/03 15:42:22 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 9, ShuffleMapStage 8)
22/04/03 15:42:22 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 9)
22/04/03 15:42:22 INFO DAGScheduler: Submitting ShuffleMapStage 9 (ShuffledRDD[3] at groupByKey at Graph.scala:138), which has no missing parents
22/04/03 15:42:22 INFO MemoryStore: Block broadcast_6 stored as values in memory (estimated size 8.5 KiB, free 434.1 MiB)
22/04/03 15:42:22 INFO MemoryStore: Block broadcast_6_piece0 stored as bytes in memory (estimated size 4.6 KiB, free 434.1 MiB)
22/04/03 15:42:22 INFO BlockManagerInfo: Added broadcast_6_piece0 in memory on 10.219.149.130:50047 (size: 4.6 KiB, free: 434.3 MiB)
22/04/03 15:42:22 INFO SparkContext: Created broadcast 6 from broadcast at DAGScheduler.scala:1427
22/04/03 15:42:22 INFO DAGScheduler: Submitting 2 missing tasks from ShuffleMapStage 9 (ShuffledRDD[3] at groupByKey at Graph.scala:138) (first 15 tasks are for partitions Vector(0, 1))
22/04/03 15:42:22 INFO TaskSchedulerImpl: Adding task set 9.0 with 2 tasks resource profile 0
22/04/03 15:42:22 INFO TaskSetManager: Starting task 0.0 in stage 9.0 (TID 10) (10.219.149.130, executor driver, partition 0, NODE_LOCAL, 4260 bytes) taskResourceAssignments Map()
22/04/03 15:42:22 INFO TaskSetManager: Starting task 1.0 in stage 9.0 (TID 11) (10.219.149.130, executor driver, partition 1, NODE_LOCAL, 4260 bytes) taskResourceAssignments Map()
22/04/03 15:42:22 INFO Executor: Running task 1.0 in stage 9.0 (TID 11)
22/04/03 15:42:22 INFO Executor: Running task 0.0 in stage 9.0 (TID 10)
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Getting 2 (108.0 B) non-empty blocks including 2 (108.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Getting 2 (103.0 B) non-empty blocks including 2 (103.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:22 INFO Executor: Finished task 1.0 in stage 9.0 (TID 11). 1465 bytes result sent to driver
22/04/03 15:42:22 INFO Executor: Finished task 0.0 in stage 9.0 (TID 10). 1465 bytes result sent to driver
22/04/03 15:42:22 INFO TaskSetManager: Finished task 1.0 in stage 9.0 (TID 11) in 10 ms on 10.219.149.130 (executor driver) (1/2)
22/04/03 15:42:22 INFO TaskSetManager: Finished task 0.0 in stage 9.0 (TID 10) in 11 ms on 10.219.149.130 (executor driver) (2/2)
22/04/03 15:42:22 INFO TaskSchedulerImpl: Removed TaskSet 9.0, whose tasks have all completed, from pool
22/04/03 15:42:22 INFO DAGScheduler: ShuffleMapStage 9 (groupByKey at Graph.scala:138) finished in 0.017 s
22/04/03 15:42:22 INFO DAGScheduler: looking for newly runnable stages
22/04/03 15:42:22 INFO DAGScheduler: running: Set()
22/04/03 15:42:22 INFO DAGScheduler: waiting: Set(ResultStage 10)
22/04/03 15:42:22 INFO DAGScheduler: failed: Set()
22/04/03 15:42:22 INFO DAGScheduler: Submitting ResultStage 10 (MapPartitionsRDD[10] at join at Graph.scala:148), which has no missing parents
22/04/03 15:42:22 INFO MemoryStore: Block broadcast_7 stored as values in memory (estimated size 6.7 KiB, free 434.1 MiB)
22/04/03 15:42:22 INFO MemoryStore: Block broadcast_7_piece0 stored as bytes in memory (estimated size 3.8 KiB, free 434.1 MiB)
22/04/03 15:42:22 INFO BlockManagerInfo: Added broadcast_7_piece0 in memory on 10.219.149.130:50047 (size: 3.8 KiB, free: 434.3 MiB)
22/04/03 15:42:22 INFO SparkContext: Created broadcast 7 from broadcast at DAGScheduler.scala:1427
22/04/03 15:42:22 INFO DAGScheduler: Submitting 2 missing tasks from ResultStage 10 (MapPartitionsRDD[10] at join at Graph.scala:148) (first 15 tasks are for partitions Vector(0, 1))
22/04/03 15:42:22 INFO TaskSchedulerImpl: Adding task set 10.0 with 2 tasks resource profile 0
22/04/03 15:42:22 INFO TaskSetManager: Starting task 0.0 in stage 10.0 (TID 12) (10.219.149.130, executor driver, partition 0, NODE_LOCAL, 4488 bytes) taskResourceAssignments Map()
22/04/03 15:42:22 INFO TaskSetManager: Starting task 1.0 in stage 10.0 (TID 13) (10.219.149.130, executor driver, partition 1, NODE_LOCAL, 4488 bytes) taskResourceAssignments Map()
22/04/03 15:42:22 INFO Executor: Running task 1.0 in stage 10.0 (TID 13)
22/04/03 15:42:22 INFO Executor: Running task 0.0 in stage 10.0 (TID 12)
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Getting 2 (108.0 B) non-empty blocks including 2 (108.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Getting 2 (114.0 B) non-empty blocks including 2 (114.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Getting 2 (890.0 B) non-empty blocks including 2 (890.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Getting 2 (890.0 B) non-empty blocks including 2 (890.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:22 INFO Executor: Finished task 0.0 in stage 10.0 (TID 12). 1938 bytes result sent to driver
22/04/03 15:42:22 INFO Executor: Finished task 1.0 in stage 10.0 (TID 13). 1911 bytes result sent to driver
22/04/03 15:42:22 INFO TaskSetManager: Finished task 1.0 in stage 10.0 (TID 13) in 16 ms on 10.219.149.130 (executor driver) (1/2)
22/04/03 15:42:22 INFO TaskSetManager: Finished task 0.0 in stage 10.0 (TID 12) in 18 ms on 10.219.149.130 (executor driver) (2/2)
22/04/03 15:42:22 INFO TaskSchedulerImpl: Removed TaskSet 10.0, whose tasks have all completed, from pool
22/04/03 15:42:22 INFO DAGScheduler: ResultStage 10 (collect at Graph.scala:150) finished in 0.024 s
22/04/03 15:42:22 INFO DAGScheduler: Job 3 is finished. Cancelling potential speculative or zombie tasks for this job
22/04/03 15:42:22 INFO TaskSchedulerImpl: Killing all running tasks in stage 10: Stage finished
22/04/03 15:42:22 INFO DAGScheduler: Job 3 finished: collect at Graph.scala:150, took 0.046738 s
(1,(0,CompactBuffer(2, 3)))
(3,(2147483647,CompactBuffer(4, 5)))
(2,(2147483647,CompactBuffer(3, 4)))
(4,(2147483647,CompactBuffer(6)))
(7,(2147483647,CompactBuffer(3)))
(5,(2147483647,CompactBuffer(1)))
For i = 0
updateDist
22/04/03 15:42:22 INFO SparkContext: Starting job: collect at Graph.scala:157
22/04/03 15:42:22 INFO DAGScheduler: Registering RDD 11 (flatMap at Graph.scala:155) as input to shuffle 3
22/04/03 15:42:22 INFO DAGScheduler: Got job 4 (collect at Graph.scala:157) with 2 output partitions
22/04/03 15:42:22 INFO DAGScheduler: Final stage: ResultStage 15 (collect at Graph.scala:157)
22/04/03 15:42:22 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 14)
22/04/03 15:42:22 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 14)
22/04/03 15:42:22 INFO DAGScheduler: Submitting ShuffleMapStage 14 (MapPartitionsRDD[11] at flatMap at Graph.scala:155), which has no missing parents
22/04/03 15:42:22 INFO MemoryStore: Block broadcast_8 stored as values in memory (estimated size 7.0 KiB, free 434.1 MiB)
22/04/03 15:42:22 INFO MemoryStore: Block broadcast_8_piece0 stored as bytes in memory (estimated size 4.0 KiB, free 434.1 MiB)
22/04/03 15:42:22 INFO BlockManagerInfo: Added broadcast_8_piece0 in memory on 10.219.149.130:50047 (size: 4.0 KiB, free: 434.3 MiB)
22/04/03 15:42:22 INFO SparkContext: Created broadcast 8 from broadcast at DAGScheduler.scala:1427
22/04/03 15:42:22 INFO DAGScheduler: Submitting 2 missing tasks from ShuffleMapStage 14 (MapPartitionsRDD[11] at flatMap at Graph.scala:155) (first 15 tasks are for partitions Vector(0, 1))
22/04/03 15:42:22 INFO TaskSchedulerImpl: Adding task set 14.0 with 2 tasks resource profile 0
22/04/03 15:42:22 INFO TaskSetManager: Starting task 0.0 in stage 14.0 (TID 14) (10.219.149.130, executor driver, partition 0, NODE_LOCAL, 4477 bytes) taskResourceAssignments Map()
22/04/03 15:42:22 INFO TaskSetManager: Starting task 1.0 in stage 14.0 (TID 15) (10.219.149.130, executor driver, partition 1, NODE_LOCAL, 4477 bytes) taskResourceAssignments Map()
22/04/03 15:42:22 INFO Executor: Running task 1.0 in stage 14.0 (TID 15)
22/04/03 15:42:22 INFO Executor: Running task 0.0 in stage 14.0 (TID 14)
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Getting 2 (114.0 B) non-empty blocks including 2 (114.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Getting 2 (108.0 B) non-empty blocks including 2 (108.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Getting 2 (890.0 B) non-empty blocks including 2 (890.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Getting 2 (890.0 B) non-empty blocks including 2 (890.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
newList =List((4,(2147483647,CompactBuffer(6))))
newList =List((7,(2147483647,CompactBuffer(3))))
newList =List((5,(2147483647,CompactBuffer(1))))
newList =List((1,(0,CompactBuffer(2, 3))), (2,(1,List())), (3,(1,List())))
newList =List((3,(2147483647,CompactBuffer(4, 5))))
newList =List((2,(2147483647,CompactBuffer(3, 4))))
22/04/03 15:42:22 INFO Executor: Finished task 1.0 in stage 14.0 (TID 15). 1465 bytes result sent to driver
22/04/03 15:42:22 INFO Executor: Finished task 0.0 in stage 14.0 (TID 14). 1465 bytes result sent to driver
22/04/03 15:42:22 INFO TaskSetManager: Finished task 1.0 in stage 14.0 (TID 15) in 21 ms on 10.219.149.130 (executor driver) (1/2)
22/04/03 15:42:22 INFO TaskSetManager: Finished task 0.0 in stage 14.0 (TID 14) in 21 ms on 10.219.149.130 (executor driver) (2/2)
22/04/03 15:42:22 INFO TaskSchedulerImpl: Removed TaskSet 14.0, whose tasks have all completed, from pool
22/04/03 15:42:22 INFO DAGScheduler: ShuffleMapStage 14 (flatMap at Graph.scala:155) finished in 0.027 s
22/04/03 15:42:22 INFO DAGScheduler: looking for newly runnable stages
22/04/03 15:42:22 INFO DAGScheduler: running: Set()
22/04/03 15:42:22 INFO DAGScheduler: waiting: Set(ResultStage 15)
22/04/03 15:42:22 INFO DAGScheduler: failed: Set()
22/04/03 15:42:22 INFO DAGScheduler: Submitting ResultStage 15 (ShuffledRDD[12] at reduceByKey at Graph.scala:155), which has no missing parents
22/04/03 15:42:22 INFO MemoryStore: Block broadcast_9 stored as values in memory (estimated size 4.6 KiB, free 434.1 MiB)
22/04/03 15:42:22 INFO MemoryStore: Block broadcast_9_piece0 stored as bytes in memory (estimated size 2.7 KiB, free 434.1 MiB)
22/04/03 15:42:22 INFO BlockManagerInfo: Added broadcast_9_piece0 in memory on 10.219.149.130:50047 (size: 2.7 KiB, free: 434.3 MiB)
22/04/03 15:42:22 INFO SparkContext: Created broadcast 9 from broadcast at DAGScheduler.scala:1427
22/04/03 15:42:22 INFO DAGScheduler: Submitting 2 missing tasks from ResultStage 15 (ShuffledRDD[12] at reduceByKey at Graph.scala:155) (first 15 tasks are for partitions Vector(0, 1))
22/04/03 15:42:22 INFO TaskSchedulerImpl: Adding task set 15.0 with 2 tasks resource profile 0
22/04/03 15:42:22 INFO TaskSetManager: Starting task 0.0 in stage 15.0 (TID 16) (10.219.149.130, executor driver, partition 0, NODE_LOCAL, 4271 bytes) taskResourceAssignments Map()
22/04/03 15:42:22 INFO TaskSetManager: Starting task 1.0 in stage 15.0 (TID 17) (10.219.149.130, executor driver, partition 1, NODE_LOCAL, 4271 bytes) taskResourceAssignments Map()
22/04/03 15:42:22 INFO Executor: Running task 0.0 in stage 15.0 (TID 16)
22/04/03 15:42:22 INFO Executor: Running task 1.0 in stage 15.0 (TID 17)
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Getting 2 (1078.0 B) non-empty blocks including 2 (1078.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Getting 2 (980.0 B) non-empty blocks including 2 (980.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:22 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:22 INFO Executor: Finished task 0.0 in stage 15.0 (TID 16). 1861 bytes result sent to driver
22/04/03 15:42:23 INFO Executor: Finished task 1.0 in stage 15.0 (TID 17). 1983 bytes result sent to driver
22/04/03 15:42:23 INFO TaskSetManager: Finished task 0.0 in stage 15.0 (TID 16) in 6 ms on 10.219.149.130 (executor driver) (1/2)
22/04/03 15:42:23 INFO TaskSetManager: Finished task 1.0 in stage 15.0 (TID 17) in 6 ms on 10.219.149.130 (executor driver) (2/2)
22/04/03 15:42:23 INFO TaskSchedulerImpl: Removed TaskSet 15.0, whose tasks have all completed, from pool
22/04/03 15:42:23 INFO DAGScheduler: ResultStage 15 (collect at Graph.scala:157) finished in 0.015 s
22/04/03 15:42:23 INFO DAGScheduler: Job 4 is finished. Cancelling potential speculative or zombie tasks for this job
22/04/03 15:42:23 INFO TaskSchedulerImpl: Killing all running tasks in stage 15: Stage finished
22/04/03 15:42:23 INFO DAGScheduler: Job 4 finished: collect at Graph.scala:157, took 0.047693 s
(4,(2147483647,CompactBuffer(6)))
(2,(1,CompactBuffer(3, 4)))
(1,(0,CompactBuffer(2, 3)))
(3,(1,CompactBuffer(4, 5)))
(7,(2147483647,CompactBuffer(3)))
(5,(2147483647,CompactBuffer(1)))
For i = 1
updateDist
22/04/03 15:42:23 INFO SparkContext: Starting job: collect at Graph.scala:157
22/04/03 15:42:23 INFO DAGScheduler: Registering RDD 13 (flatMap at Graph.scala:155) as input to shuffle 4
22/04/03 15:42:23 INFO DAGScheduler: Got job 5 (collect at Graph.scala:157) with 2 output partitions
22/04/03 15:42:23 INFO DAGScheduler: Final stage: ResultStage 21 (collect at Graph.scala:157)
22/04/03 15:42:23 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 20)
22/04/03 15:42:23 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 20)
22/04/03 15:42:23 INFO DAGScheduler: Submitting ShuffleMapStage 20 (MapPartitionsRDD[13] at flatMap at Graph.scala:155), which has no missing parents
22/04/03 15:42:23 INFO MemoryStore: Block broadcast_10 stored as values in memory (estimated size 5.1 KiB, free 434.0 MiB)
22/04/03 15:42:23 INFO MemoryStore: Block broadcast_10_piece0 stored as bytes in memory (estimated size 2.9 KiB, free 434.0 MiB)
22/04/03 15:42:23 INFO BlockManagerInfo: Added broadcast_10_piece0 in memory on 10.219.149.130:50047 (size: 2.9 KiB, free: 434.3 MiB)
22/04/03 15:42:23 INFO SparkContext: Created broadcast 10 from broadcast at DAGScheduler.scala:1427
22/04/03 15:42:23 INFO DAGScheduler: Submitting 2 missing tasks from ShuffleMapStage 20 (MapPartitionsRDD[13] at flatMap at Graph.scala:155) (first 15 tasks are for partitions Vector(0, 1))
22/04/03 15:42:23 INFO TaskSchedulerImpl: Adding task set 20.0 with 2 tasks resource profile 0
22/04/03 15:42:23 INFO TaskSetManager: Starting task 0.0 in stage 20.0 (TID 18) (10.219.149.130, executor driver, partition 0, NODE_LOCAL, 4260 bytes) taskResourceAssignments Map()
22/04/03 15:42:23 INFO TaskSetManager: Starting task 1.0 in stage 20.0 (TID 19) (10.219.149.130, executor driver, partition 1, NODE_LOCAL, 4260 bytes) taskResourceAssignments Map()
22/04/03 15:42:23 INFO Executor: Running task 1.0 in stage 20.0 (TID 19)
22/04/03 15:42:23 INFO Executor: Running task 0.0 in stage 20.0 (TID 18)
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Getting 2 (1078.0 B) non-empty blocks including 2 (1078.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Getting 2 (980.0 B) non-empty blocks including 2 (980.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
newList =List((1,(0,CompactBuffer(2, 3))), (2,(1,List())), (3,(1,List())))
newList =List((3,(1,CompactBuffer(4, 5))), (4,(2,List())), (5,(2,List())))
newList =List((7,(2147483647,CompactBuffer(3))))
newList =List((4,(2147483647,CompactBuffer(6))))
newList =List((5,(2147483647,CompactBuffer(1))))
newList =List((2,(1,CompactBuffer(3, 4))), (3,(2,List())), (4,(2,List())))
22/04/03 15:42:23 INFO Executor: Finished task 1.0 in stage 20.0 (TID 19). 1465 bytes result sent to driver
22/04/03 15:42:23 INFO Executor: Finished task 0.0 in stage 20.0 (TID 18). 1465 bytes result sent to driver
22/04/03 15:42:23 INFO TaskSetManager: Finished task 0.0 in stage 20.0 (TID 18) in 9 ms on 10.219.149.130 (executor driver) (1/2)
22/04/03 15:42:23 INFO TaskSetManager: Finished task 1.0 in stage 20.0 (TID 19) in 9 ms on 10.219.149.130 (executor driver) (2/2)
22/04/03 15:42:23 INFO TaskSchedulerImpl: Removed TaskSet 20.0, whose tasks have all completed, from pool
22/04/03 15:42:23 INFO DAGScheduler: ShuffleMapStage 20 (flatMap at Graph.scala:155) finished in 0.014 s
22/04/03 15:42:23 INFO DAGScheduler: looking for newly runnable stages
22/04/03 15:42:23 INFO DAGScheduler: running: Set()
22/04/03 15:42:23 INFO DAGScheduler: waiting: Set(ResultStage 21)
22/04/03 15:42:23 INFO DAGScheduler: failed: Set()
22/04/03 15:42:23 INFO DAGScheduler: Submitting ResultStage 21 (ShuffledRDD[14] at reduceByKey at Graph.scala:155), which has no missing parents
22/04/03 15:42:23 INFO MemoryStore: Block broadcast_11 stored as values in memory (estimated size 4.6 KiB, free 434.0 MiB)
22/04/03 15:42:23 INFO MemoryStore: Block broadcast_11_piece0 stored as bytes in memory (estimated size 2.7 KiB, free 434.0 MiB)
22/04/03 15:42:23 INFO BlockManagerInfo: Added broadcast_11_piece0 in memory on 10.219.149.130:50047 (size: 2.7 KiB, free: 434.3 MiB)
22/04/03 15:42:23 INFO SparkContext: Created broadcast 11 from broadcast at DAGScheduler.scala:1427
22/04/03 15:42:23 INFO DAGScheduler: Submitting 2 missing tasks from ResultStage 21 (ShuffledRDD[14] at reduceByKey at Graph.scala:155) (first 15 tasks are for partitions Vector(0, 1))
22/04/03 15:42:23 INFO TaskSchedulerImpl: Adding task set 21.0 with 2 tasks resource profile 0
22/04/03 15:42:23 INFO TaskSetManager: Starting task 0.0 in stage 21.0 (TID 20) (10.219.149.130, executor driver, partition 0, NODE_LOCAL, 4271 bytes) taskResourceAssignments Map()
22/04/03 15:42:23 INFO TaskSetManager: Starting task 1.0 in stage 21.0 (TID 21) (10.219.149.130, executor driver, partition 1, NODE_LOCAL, 4271 bytes) taskResourceAssignments Map()
22/04/03 15:42:23 INFO Executor: Running task 0.0 in stage 21.0 (TID 20)
22/04/03 15:42:23 INFO Executor: Running task 1.0 in stage 21.0 (TID 21)
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Getting 2 (843.0 B) non-empty blocks including 2 (843.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Getting 2 (897.0 B) non-empty blocks including 2 (897.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:23 INFO Executor: Finished task 0.0 in stage 21.0 (TID 20). 1861 bytes result sent to driver
22/04/03 15:42:23 INFO TaskSetManager: Finished task 0.0 in stage 21.0 (TID 20) in 6 ms on 10.219.149.130 (executor driver) (1/2)
22/04/03 15:42:23 INFO Executor: Finished task 1.0 in stage 21.0 (TID 21). 1988 bytes result sent to driver
22/04/03 15:42:23 INFO TaskSetManager: Finished task 1.0 in stage 21.0 (TID 21) in 8 ms on 10.219.149.130 (executor driver) (2/2)
22/04/03 15:42:23 INFO TaskSchedulerImpl: Removed TaskSet 21.0, whose tasks have all completed, from pool
22/04/03 15:42:23 INFO DAGScheduler: ResultStage 21 (collect at Graph.scala:157) finished in 0.012 s
22/04/03 15:42:23 INFO DAGScheduler: Job 5 is finished. Cancelling potential speculative or zombie tasks for this job
22/04/03 15:42:23 INFO TaskSchedulerImpl: Killing all running tasks in stage 21: Stage finished
22/04/03 15:42:23 INFO DAGScheduler: Job 5 finished: collect at Graph.scala:157, took 0.032162 s
(4,(2,CompactBuffer(6)))
(2,(1,CompactBuffer(3, 4)))
(1,(0,CompactBuffer(2, 3)))
(3,(1,CompactBuffer(4, 5)))
(7,(2147483647,CompactBuffer(3)))
(5,(2,CompactBuffer(1)))
For i = 2
updateDist
22/04/03 15:42:23 INFO SparkContext: Starting job: collect at Graph.scala:157
22/04/03 15:42:23 INFO DAGScheduler: Registering RDD 15 (flatMap at Graph.scala:155) as input to shuffle 5
22/04/03 15:42:23 INFO DAGScheduler: Got job 6 (collect at Graph.scala:157) with 2 output partitions
22/04/03 15:42:23 INFO DAGScheduler: Final stage: ResultStage 28 (collect at Graph.scala:157)
22/04/03 15:42:23 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 27)
22/04/03 15:42:23 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 27)
22/04/03 15:42:23 INFO DAGScheduler: Submitting ShuffleMapStage 27 (MapPartitionsRDD[15] at flatMap at Graph.scala:155), which has no missing parents
22/04/03 15:42:23 INFO MemoryStore: Block broadcast_12 stored as values in memory (estimated size 5.1 KiB, free 434.0 MiB)
22/04/03 15:42:23 INFO MemoryStore: Block broadcast_12_piece0 stored as bytes in memory (estimated size 2.9 KiB, free 434.0 MiB)
22/04/03 15:42:23 INFO BlockManagerInfo: Added broadcast_12_piece0 in memory on 10.219.149.130:50047 (size: 2.9 KiB, free: 434.3 MiB)
22/04/03 15:42:23 INFO SparkContext: Created broadcast 12 from broadcast at DAGScheduler.scala:1427
22/04/03 15:42:23 INFO DAGScheduler: Submitting 2 missing tasks from ShuffleMapStage 27 (MapPartitionsRDD[15] at flatMap at Graph.scala:155) (first 15 tasks are for partitions Vector(0, 1))
22/04/03 15:42:23 INFO TaskSchedulerImpl: Adding task set 27.0 with 2 tasks resource profile 0
22/04/03 15:42:23 INFO TaskSetManager: Starting task 0.0 in stage 27.0 (TID 22) (10.219.149.130, executor driver, partition 0, NODE_LOCAL, 4260 bytes) taskResourceAssignments Map()
22/04/03 15:42:23 INFO TaskSetManager: Starting task 1.0 in stage 27.0 (TID 23) (10.219.149.130, executor driver, partition 1, NODE_LOCAL, 4260 bytes) taskResourceAssignments Map()
22/04/03 15:42:23 INFO Executor: Running task 0.0 in stage 27.0 (TID 22)
22/04/03 15:42:23 INFO Executor: Running task 1.0 in stage 27.0 (TID 23)
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Getting 2 (843.0 B) non-empty blocks including 2 (843.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Getting 2 (897.0 B) non-empty blocks including 2 (897.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
newList =List((4,(2,CompactBuffer(6))), (6,(3,List())))
newList =List((2,(1,CompactBuffer(3, 4))), (3,(2,List())), (4,(2,List())))
22/04/03 15:42:23 INFO Executor: Finished task 0.0 in stage 27.0 (TID 22). 1465 bytes result sent to driver
newList =List((1,(0,CompactBuffer(2, 3))), (2,(1,List())), (3,(1,List())))
newList =List((3,(1,CompactBuffer(4, 5))), (4,(2,List())), (5,(2,List())))
newList =List((7,(2147483647,CompactBuffer(3))))
22/04/03 15:42:23 INFO TaskSetManager: Finished task 0.0 in stage 27.0 (TID 22) in 10 ms on 10.219.149.130 (executor driver) (1/2)
newList =List((5,(2,CompactBuffer(1))), (1,(3,List())))
22/04/03 15:42:23 INFO Executor: Finished task 1.0 in stage 27.0 (TID 23). 1465 bytes result sent to driver
22/04/03 15:42:23 INFO TaskSetManager: Finished task 1.0 in stage 27.0 (TID 23) in 14 ms on 10.219.149.130 (executor driver) (2/2)
22/04/03 15:42:23 INFO TaskSchedulerImpl: Removed TaskSet 27.0, whose tasks have all completed, from pool
22/04/03 15:42:23 INFO DAGScheduler: ShuffleMapStage 27 (flatMap at Graph.scala:155) finished in 0.019 s
22/04/03 15:42:23 INFO DAGScheduler: looking for newly runnable stages
22/04/03 15:42:23 INFO DAGScheduler: running: Set()
22/04/03 15:42:23 INFO DAGScheduler: waiting: Set(ResultStage 28)
22/04/03 15:42:23 INFO DAGScheduler: failed: Set()
22/04/03 15:42:23 INFO DAGScheduler: Submitting ResultStage 28 (ShuffledRDD[16] at reduceByKey at Graph.scala:155), which has no missing parents
22/04/03 15:42:23 INFO MemoryStore: Block broadcast_13 stored as values in memory (estimated size 4.6 KiB, free 434.0 MiB)
22/04/03 15:42:23 INFO MemoryStore: Block broadcast_13_piece0 stored as bytes in memory (estimated size 2.7 KiB, free 434.0 MiB)
22/04/03 15:42:23 INFO BlockManagerInfo: Added broadcast_13_piece0 in memory on 10.219.149.130:50047 (size: 2.7 KiB, free: 434.3 MiB)
22/04/03 15:42:23 INFO SparkContext: Created broadcast 13 from broadcast at DAGScheduler.scala:1427
22/04/03 15:42:23 INFO DAGScheduler: Submitting 2 missing tasks from ResultStage 28 (ShuffledRDD[16] at reduceByKey at Graph.scala:155) (first 15 tasks are for partitions Vector(0, 1))
22/04/03 15:42:23 INFO TaskSchedulerImpl: Adding task set 28.0 with 2 tasks resource profile 0
22/04/03 15:42:23 INFO TaskSetManager: Starting task 0.0 in stage 28.0 (TID 24) (10.219.149.130, executor driver, partition 0, NODE_LOCAL, 4271 bytes) taskResourceAssignments Map()
22/04/03 15:42:23 INFO TaskSetManager: Starting task 1.0 in stage 28.0 (TID 25) (10.219.149.130, executor driver, partition 1, NODE_LOCAL, 4271 bytes) taskResourceAssignments Map()
22/04/03 15:42:23 INFO Executor: Running task 0.0 in stage 28.0 (TID 24)
22/04/03 15:42:23 INFO Executor: Running task 1.0 in stage 28.0 (TID 25)
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Getting 2 (897.0 B) non-empty blocks including 2 (897.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Getting 2 (897.0 B) non-empty blocks including 2 (897.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:23 INFO Executor: Finished task 0.0 in stage 28.0 (TID 24). 2022 bytes result sent to driver
22/04/03 15:42:23 INFO Executor: Finished task 1.0 in stage 28.0 (TID 25). 1988 bytes result sent to driver
22/04/03 15:42:23 INFO TaskSetManager: Finished task 0.0 in stage 28.0 (TID 24) in 7 ms on 10.219.149.130 (executor driver) (1/2)
22/04/03 15:42:23 INFO TaskSetManager: Finished task 1.0 in stage 28.0 (TID 25) in 6 ms on 10.219.149.130 (executor driver) (2/2)
22/04/03 15:42:23 INFO TaskSchedulerImpl: Removed TaskSet 28.0, whose tasks have all completed, from pool
22/04/03 15:42:23 INFO DAGScheduler: ResultStage 28 (collect at Graph.scala:157) finished in 0.011 s
22/04/03 15:42:23 INFO DAGScheduler: Job 6 is finished. Cancelling potential speculative or zombie tasks for this job
22/04/03 15:42:23 INFO TaskSchedulerImpl: Killing all running tasks in stage 28: Stage finished
22/04/03 15:42:23 INFO DAGScheduler: Job 6 finished: collect at Graph.scala:157, took 0.034181 s
(4,(2,CompactBuffer(6)))
(6,(3,List()))
(2,(1,CompactBuffer(3, 4)))
(1,(0,CompactBuffer(2, 3)))
(3,(1,CompactBuffer(4, 5)))
(7,(2147483647,CompactBuffer(3)))
(5,(2,CompactBuffer(1)))
For i = 3
updateDist
22/04/03 15:42:23 INFO SparkContext: Starting job: collect at Graph.scala:157
22/04/03 15:42:23 INFO DAGScheduler: Registering RDD 17 (flatMap at Graph.scala:155) as input to shuffle 6
22/04/03 15:42:23 INFO DAGScheduler: Got job 7 (collect at Graph.scala:157) with 2 output partitions
22/04/03 15:42:23 INFO DAGScheduler: Final stage: ResultStage 36 (collect at Graph.scala:157)
22/04/03 15:42:23 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 35)
22/04/03 15:42:23 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 35)
22/04/03 15:42:23 INFO DAGScheduler: Submitting ShuffleMapStage 35 (MapPartitionsRDD[17] at flatMap at Graph.scala:155), which has no missing parents
22/04/03 15:42:23 INFO MemoryStore: Block broadcast_14 stored as values in memory (estimated size 5.1 KiB, free 434.0 MiB)
22/04/03 15:42:23 INFO MemoryStore: Block broadcast_14_piece0 stored as bytes in memory (estimated size 2.9 KiB, free 434.0 MiB)
22/04/03 15:42:23 INFO BlockManagerInfo: Added broadcast_14_piece0 in memory on 10.219.149.130:50047 (size: 2.9 KiB, free: 434.3 MiB)
22/04/03 15:42:23 INFO SparkContext: Created broadcast 14 from broadcast at DAGScheduler.scala:1427
22/04/03 15:42:23 INFO DAGScheduler: Submitting 2 missing tasks from ShuffleMapStage 35 (MapPartitionsRDD[17] at flatMap at Graph.scala:155) (first 15 tasks are for partitions Vector(0, 1))
22/04/03 15:42:23 INFO TaskSchedulerImpl: Adding task set 35.0 with 2 tasks resource profile 0
22/04/03 15:42:23 INFO TaskSetManager: Starting task 0.0 in stage 35.0 (TID 26) (10.219.149.130, executor driver, partition 0, NODE_LOCAL, 4260 bytes) taskResourceAssignments Map()
22/04/03 15:42:23 INFO TaskSetManager: Starting task 1.0 in stage 35.0 (TID 27) (10.219.149.130, executor driver, partition 1, NODE_LOCAL, 4260 bytes) taskResourceAssignments Map()
22/04/03 15:42:23 INFO Executor: Running task 0.0 in stage 35.0 (TID 26)
22/04/03 15:42:23 INFO Executor: Running task 1.0 in stage 35.0 (TID 27)
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Getting 2 (897.0 B) non-empty blocks including 2 (897.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Getting 2 (897.0 B) non-empty blocks including 2 (897.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
newList =List((4,(2,CompactBuffer(6))), (6,(3,List())))
newList =List((6,(3,List())))
newList =List((2,(1,CompactBuffer(3, 4))), (3,(2,List())), (4,(2,List())))
newList =List((1,(0,CompactBuffer(2, 3))), (2,(1,List())), (3,(1,List())))
newList =List((3,(1,CompactBuffer(4, 5))), (4,(2,List())), (5,(2,List())))
newList =List((7,(2147483647,CompactBuffer(3))))
newList =List((5,(2,CompactBuffer(1))), (1,(3,List())))
22/04/03 15:42:23 INFO Executor: Finished task 0.0 in stage 35.0 (TID 26). 1465 bytes result sent to driver
22/04/03 15:42:23 INFO TaskSetManager: Finished task 0.0 in stage 35.0 (TID 26) in 8 ms on 10.219.149.130 (executor driver) (1/2)
22/04/03 15:42:23 INFO Executor: Finished task 1.0 in stage 35.0 (TID 27). 1465 bytes result sent to driver
22/04/03 15:42:23 INFO TaskSetManager: Finished task 1.0 in stage 35.0 (TID 27) in 8 ms on 10.219.149.130 (executor driver) (2/2)
22/04/03 15:42:23 INFO TaskSchedulerImpl: Removed TaskSet 35.0, whose tasks have all completed, from pool
22/04/03 15:42:23 INFO DAGScheduler: ShuffleMapStage 35 (flatMap at Graph.scala:155) finished in 0.014 s
22/04/03 15:42:23 INFO DAGScheduler: looking for newly runnable stages
22/04/03 15:42:23 INFO DAGScheduler: running: Set()
22/04/03 15:42:23 INFO DAGScheduler: waiting: Set(ResultStage 36)
22/04/03 15:42:23 INFO DAGScheduler: failed: Set()
22/04/03 15:42:23 INFO DAGScheduler: Submitting ResultStage 36 (ShuffledRDD[18] at reduceByKey at Graph.scala:155), which has no missing parents
22/04/03 15:42:23 INFO MemoryStore: Block broadcast_15 stored as values in memory (estimated size 4.6 KiB, free 434.0 MiB)
22/04/03 15:42:23 INFO MemoryStore: Block broadcast_15_piece0 stored as bytes in memory (estimated size 2.7 KiB, free 434.0 MiB)
22/04/03 15:42:23 INFO BlockManagerInfo: Added broadcast_15_piece0 in memory on 10.219.149.130:50047 (size: 2.7 KiB, free: 434.3 MiB)
22/04/03 15:42:23 INFO SparkContext: Created broadcast 15 from broadcast at DAGScheduler.scala:1427
22/04/03 15:42:23 INFO DAGScheduler: Submitting 2 missing tasks from ResultStage 36 (ShuffledRDD[18] at reduceByKey at Graph.scala:155) (first 15 tasks are for partitions Vector(0, 1))
22/04/03 15:42:23 INFO TaskSchedulerImpl: Adding task set 36.0 with 2 tasks resource profile 0
22/04/03 15:42:23 INFO TaskSetManager: Starting task 0.0 in stage 36.0 (TID 28) (10.219.149.130, executor driver, partition 0, NODE_LOCAL, 4271 bytes) taskResourceAssignments Map()
22/04/03 15:42:23 INFO TaskSetManager: Starting task 1.0 in stage 36.0 (TID 29) (10.219.149.130, executor driver, partition 1, NODE_LOCAL, 4271 bytes) taskResourceAssignments Map()
22/04/03 15:42:23 INFO Executor: Running task 1.0 in stage 36.0 (TID 29)
22/04/03 15:42:23 INFO Executor: Running task 0.0 in stage 36.0 (TID 28)
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Getting 2 (897.0 B) non-empty blocks including 2 (897.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Getting 2 (897.0 B) non-empty blocks including 2 (897.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:23 INFO Executor: Finished task 0.0 in stage 36.0 (TID 28). 2022 bytes result sent to driver
22/04/03 15:42:23 INFO Executor: Finished task 1.0 in stage 36.0 (TID 29). 1988 bytes result sent to driver
22/04/03 15:42:23 INFO TaskSetManager: Finished task 1.0 in stage 36.0 (TID 29) in 5 ms on 10.219.149.130 (executor driver) (1/2)
22/04/03 15:42:23 INFO TaskSetManager: Finished task 0.0 in stage 36.0 (TID 28) in 5 ms on 10.219.149.130 (executor driver) (2/2)
22/04/03 15:42:23 INFO TaskSchedulerImpl: Removed TaskSet 36.0, whose tasks have all completed, from pool
22/04/03 15:42:23 INFO DAGScheduler: ResultStage 36 (collect at Graph.scala:157) finished in 0.008 s
22/04/03 15:42:23 INFO DAGScheduler: Job 7 is finished. Cancelling potential speculative or zombie tasks for this job
22/04/03 15:42:23 INFO TaskSchedulerImpl: Killing all running tasks in stage 36: Stage finished
22/04/03 15:42:23 INFO DAGScheduler: Job 7 finished: collect at Graph.scala:157, took 0.025558 s
(4,(2,CompactBuffer(6)))
(6,(3,List()))
(2,(1,CompactBuffer(3, 4)))
(1,(0,CompactBuffer(2, 3)))
(3,(1,CompactBuffer(4, 5)))
(7,(2147483647,CompactBuffer(3)))
(5,(2,CompactBuffer(1)))
For i = 4
updateDist
22/04/03 15:42:23 INFO SparkContext: Starting job: collect at Graph.scala:157
22/04/03 15:42:23 INFO DAGScheduler: Registering RDD 19 (flatMap at Graph.scala:155) as input to shuffle 7
22/04/03 15:42:23 INFO DAGScheduler: Got job 8 (collect at Graph.scala:157) with 2 output partitions
22/04/03 15:42:23 INFO DAGScheduler: Final stage: ResultStage 45 (collect at Graph.scala:157)
22/04/03 15:42:23 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 44)
22/04/03 15:42:23 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 44)
22/04/03 15:42:23 INFO DAGScheduler: Submitting ShuffleMapStage 44 (MapPartitionsRDD[19] at flatMap at Graph.scala:155), which has no missing parents
22/04/03 15:42:23 INFO MemoryStore: Block broadcast_16 stored as values in memory (estimated size 5.1 KiB, free 434.0 MiB)
22/04/03 15:42:23 INFO MemoryStore: Block broadcast_16_piece0 stored as bytes in memory (estimated size 2.9 KiB, free 434.0 MiB)
22/04/03 15:42:23 INFO BlockManagerInfo: Added broadcast_16_piece0 in memory on 10.219.149.130:50047 (size: 2.9 KiB, free: 434.3 MiB)
22/04/03 15:42:23 INFO SparkContext: Created broadcast 16 from broadcast at DAGScheduler.scala:1427
22/04/03 15:42:23 INFO DAGScheduler: Submitting 2 missing tasks from ShuffleMapStage 44 (MapPartitionsRDD[19] at flatMap at Graph.scala:155) (first 15 tasks are for partitions Vector(0, 1))
22/04/03 15:42:23 INFO TaskSchedulerImpl: Adding task set 44.0 with 2 tasks resource profile 0
22/04/03 15:42:23 INFO TaskSetManager: Starting task 0.0 in stage 44.0 (TID 30) (10.219.149.130, executor driver, partition 0, NODE_LOCAL, 4260 bytes) taskResourceAssignments Map()
22/04/03 15:42:23 INFO TaskSetManager: Starting task 1.0 in stage 44.0 (TID 31) (10.219.149.130, executor driver, partition 1, NODE_LOCAL, 4260 bytes) taskResourceAssignments Map()
22/04/03 15:42:23 INFO Executor: Running task 0.0 in stage 44.0 (TID 30)
22/04/03 15:42:23 INFO Executor: Running task 1.0 in stage 44.0 (TID 31)
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Getting 2 (897.0 B) non-empty blocks including 2 (897.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Getting 2 (897.0 B) non-empty blocks including 2 (897.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
newList =List((4,(2,CompactBuffer(6))), (6,(3,List())))
newList =List((6,(3,List())))
newList =List((1,(0,CompactBuffer(2, 3))), (2,(1,List())), (3,(1,List())))
newList =List((2,(1,CompactBuffer(3, 4))), (3,(2,List())), (4,(2,List())))
newList =List((3,(1,CompactBuffer(4, 5))), (4,(2,List())), (5,(2,List())))
newList =List((7,(2147483647,CompactBuffer(3))))
newList =List((5,(2,CompactBuffer(1))), (1,(3,List())))
22/04/03 15:42:23 INFO Executor: Finished task 0.0 in stage 44.0 (TID 30). 1465 bytes result sent to driver
22/04/03 15:42:23 INFO TaskSetManager: Finished task 0.0 in stage 44.0 (TID 30) in 7 ms on 10.219.149.130 (executor driver) (1/2)
22/04/03 15:42:23 INFO Executor: Finished task 1.0 in stage 44.0 (TID 31). 1465 bytes result sent to driver
22/04/03 15:42:23 INFO TaskSetManager: Finished task 1.0 in stage 44.0 (TID 31) in 9 ms on 10.219.149.130 (executor driver) (2/2)
22/04/03 15:42:23 INFO TaskSchedulerImpl: Removed TaskSet 44.0, whose tasks have all completed, from pool
22/04/03 15:42:23 INFO DAGScheduler: ShuffleMapStage 44 (flatMap at Graph.scala:155) finished in 0.015 s
22/04/03 15:42:23 INFO DAGScheduler: looking for newly runnable stages
22/04/03 15:42:23 INFO DAGScheduler: running: Set()
22/04/03 15:42:23 INFO DAGScheduler: waiting: Set(ResultStage 45)
22/04/03 15:42:23 INFO DAGScheduler: failed: Set()
22/04/03 15:42:23 INFO DAGScheduler: Submitting ResultStage 45 (ShuffledRDD[20] at reduceByKey at Graph.scala:155), which has no missing parents
22/04/03 15:42:23 INFO MemoryStore: Block broadcast_17 stored as values in memory (estimated size 4.6 KiB, free 434.0 MiB)
22/04/03 15:42:23 INFO MemoryStore: Block broadcast_17_piece0 stored as bytes in memory (estimated size 2.7 KiB, free 434.0 MiB)
22/04/03 15:42:23 INFO BlockManagerInfo: Added broadcast_17_piece0 in memory on 10.219.149.130:50047 (size: 2.7 KiB, free: 434.3 MiB)
22/04/03 15:42:23 INFO SparkContext: Created broadcast 17 from broadcast at DAGScheduler.scala:1427
22/04/03 15:42:23 INFO DAGScheduler: Submitting 2 missing tasks from ResultStage 45 (ShuffledRDD[20] at reduceByKey at Graph.scala:155) (first 15 tasks are for partitions Vector(0, 1))
22/04/03 15:42:23 INFO TaskSchedulerImpl: Adding task set 45.0 with 2 tasks resource profile 0
22/04/03 15:42:23 INFO TaskSetManager: Starting task 0.0 in stage 45.0 (TID 32) (10.219.149.130, executor driver, partition 0, NODE_LOCAL, 4271 bytes) taskResourceAssignments Map()
22/04/03 15:42:23 INFO TaskSetManager: Starting task 1.0 in stage 45.0 (TID 33) (10.219.149.130, executor driver, partition 1, NODE_LOCAL, 4271 bytes) taskResourceAssignments Map()
22/04/03 15:42:23 INFO Executor: Running task 0.0 in stage 45.0 (TID 32)
22/04/03 15:42:23 INFO Executor: Running task 1.0 in stage 45.0 (TID 33)
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Getting 2 (897.0 B) non-empty blocks including 2 (897.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Getting 2 (897.0 B) non-empty blocks including 2 (897.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:23 INFO Executor: Finished task 0.0 in stage 45.0 (TID 32). 2022 bytes result sent to driver
22/04/03 15:42:23 INFO Executor: Finished task 1.0 in stage 45.0 (TID 33). 1988 bytes result sent to driver
22/04/03 15:42:23 INFO TaskSetManager: Finished task 1.0 in stage 45.0 (TID 33) in 4 ms on 10.219.149.130 (executor driver) (1/2)
22/04/03 15:42:23 INFO TaskSetManager: Finished task 0.0 in stage 45.0 (TID 32) in 4 ms on 10.219.149.130 (executor driver) (2/2)
22/04/03 15:42:23 INFO TaskSchedulerImpl: Removed TaskSet 45.0, whose tasks have all completed, from pool
22/04/03 15:42:23 INFO DAGScheduler: ResultStage 45 (collect at Graph.scala:157) finished in 0.013 s
22/04/03 15:42:23 INFO DAGScheduler: Job 8 is finished. Cancelling potential speculative or zombie tasks for this job
22/04/03 15:42:23 INFO TaskSchedulerImpl: Killing all running tasks in stage 45: Stage finished
22/04/03 15:42:23 INFO DAGScheduler: Job 8 finished: collect at Graph.scala:157, took 0.030747 s
(4,(2,CompactBuffer(6)))
(6,(3,List()))
(2,(1,CompactBuffer(3, 4)))
(1,(0,CompactBuffer(2, 3)))
(3,(1,CompactBuffer(4, 5)))
(7,(2147483647,CompactBuffer(3)))
(5,(2,CompactBuffer(1)))
For i = 5
updateDist
22/04/03 15:42:23 INFO SparkContext: Starting job: collect at Graph.scala:157
22/04/03 15:42:23 INFO DAGScheduler: Registering RDD 21 (flatMap at Graph.scala:155) as input to shuffle 8
22/04/03 15:42:23 INFO DAGScheduler: Got job 9 (collect at Graph.scala:157) with 2 output partitions
22/04/03 15:42:23 INFO DAGScheduler: Final stage: ResultStage 55 (collect at Graph.scala:157)
22/04/03 15:42:23 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 54)
22/04/03 15:42:23 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 54)
22/04/03 15:42:23 INFO DAGScheduler: Submitting ShuffleMapStage 54 (MapPartitionsRDD[21] at flatMap at Graph.scala:155), which has no missing parents
22/04/03 15:42:23 INFO BlockManagerInfo: Removed broadcast_11_piece0 on 10.219.149.130:50047 in memory (size: 2.7 KiB, free: 434.3 MiB)
22/04/03 15:42:23 INFO MemoryStore: Block broadcast_18 stored as values in memory (estimated size 5.1 KiB, free 434.0 MiB)
22/04/03 15:42:23 INFO MemoryStore: Block broadcast_18_piece0 stored as bytes in memory (estimated size 2.9 KiB, free 434.0 MiB)
22/04/03 15:42:23 INFO BlockManagerInfo: Added broadcast_18_piece0 in memory on 10.219.149.130:50047 (size: 2.9 KiB, free: 434.3 MiB)
22/04/03 15:42:23 INFO SparkContext: Created broadcast 18 from broadcast at DAGScheduler.scala:1427
22/04/03 15:42:23 INFO DAGScheduler: Submitting 2 missing tasks from ShuffleMapStage 54 (MapPartitionsRDD[21] at flatMap at Graph.scala:155) (first 15 tasks are for partitions Vector(0, 1))
22/04/03 15:42:23 INFO TaskSchedulerImpl: Adding task set 54.0 with 2 tasks resource profile 0
22/04/03 15:42:23 INFO TaskSetManager: Starting task 0.0 in stage 54.0 (TID 34) (10.219.149.130, executor driver, partition 0, NODE_LOCAL, 4260 bytes) taskResourceAssignments Map()
22/04/03 15:42:23 INFO TaskSetManager: Starting task 1.0 in stage 54.0 (TID 35) (10.219.149.130, executor driver, partition 1, NODE_LOCAL, 4260 bytes) taskResourceAssignments Map()
22/04/03 15:42:23 INFO Executor: Running task 1.0 in stage 54.0 (TID 35)
22/04/03 15:42:23 INFO Executor: Running task 0.0 in stage 54.0 (TID 34)
22/04/03 15:42:23 INFO BlockManagerInfo: Removed broadcast_14_piece0 on 10.219.149.130:50047 in memory (size: 2.9 KiB, free: 434.3 MiB)
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Getting 2 (897.0 B) non-empty blocks including 2 (897.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Getting 2 (897.0 B) non-empty blocks including 2 (897.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
newList =List((4,(2,CompactBuffer(6))), (6,(3,List())))
newList =List((1,(0,CompactBuffer(2, 3))), (2,(1,List())), (3,(1,List())))
newList =List((6,(3,List())))22/04/03 15:42:23 INFO BlockManagerInfo: Removed broadcast_13_piece0 on 10.219.149.130:50047 in memory (size: 2.7 KiB, free: 434.3 MiB)

newList =List((3,(1,CompactBuffer(4, 5))), (4,(2,List())), (5,(2,List())))
newList =List((2,(1,CompactBuffer(3, 4))), (3,(2,List())), (4,(2,List())))
newList =List((7,(2147483647,CompactBuffer(3))))
newList =List((5,(2,CompactBuffer(1))), (1,(3,List())))
22/04/03 15:42:23 INFO BlockManagerInfo: Removed broadcast_9_piece0 on 10.219.149.130:50047 in memory (size: 2.7 KiB, free: 434.3 MiB)
22/04/03 15:42:23 INFO Executor: Finished task 0.0 in stage 54.0 (TID 34). 1465 bytes result sent to driver
22/04/03 15:42:23 INFO Executor: Finished task 1.0 in stage 54.0 (TID 35). 1465 bytes result sent to driver
22/04/03 15:42:23 INFO TaskSetManager: Finished task 0.0 in stage 54.0 (TID 34) in 8 ms on 10.219.149.130 (executor driver) (1/2)
22/04/03 15:42:23 INFO TaskSetManager: Finished task 1.0 in stage 54.0 (TID 35) in 9 ms on 10.219.149.130 (executor driver) (2/2)
22/04/03 15:42:23 INFO TaskSchedulerImpl: Removed TaskSet 54.0, whose tasks have all completed, from pool
22/04/03 15:42:23 INFO DAGScheduler: ShuffleMapStage 54 (flatMap at Graph.scala:155) finished in 0.015 s
22/04/03 15:42:23 INFO DAGScheduler: looking for newly runnable stages
22/04/03 15:42:23 INFO DAGScheduler: running: Set()
22/04/03 15:42:23 INFO DAGScheduler: waiting: Set(ResultStage 55)
22/04/03 15:42:23 INFO DAGScheduler: failed: Set()
22/04/03 15:42:23 INFO DAGScheduler: Submitting ResultStage 55 (ShuffledRDD[22] at reduceByKey at Graph.scala:155), which has no missing parents
22/04/03 15:42:23 INFO MemoryStore: Block broadcast_19 stored as values in memory (estimated size 4.6 KiB, free 434.0 MiB)
22/04/03 15:42:23 INFO BlockManagerInfo: Removed broadcast_7_piece0 on 10.219.149.130:50047 in memory (size: 3.8 KiB, free: 434.3 MiB)
22/04/03 15:42:23 INFO MemoryStore: Block broadcast_19_piece0 stored as bytes in memory (estimated size 2.7 KiB, free 434.0 MiB)
22/04/03 15:42:23 INFO BlockManagerInfo: Added broadcast_19_piece0 in memory on 10.219.149.130:50047 (size: 2.7 KiB, free: 434.3 MiB)
22/04/03 15:42:23 INFO SparkContext: Created broadcast 19 from broadcast at DAGScheduler.scala:1427
22/04/03 15:42:23 INFO DAGScheduler: Submitting 2 missing tasks from ResultStage 55 (ShuffledRDD[22] at reduceByKey at Graph.scala:155) (first 15 tasks are for partitions Vector(0, 1))
22/04/03 15:42:23 INFO TaskSchedulerImpl: Adding task set 55.0 with 2 tasks resource profile 0
22/04/03 15:42:23 INFO BlockManagerInfo: Removed broadcast_10_piece0 on 10.219.149.130:50047 in memory (size: 2.9 KiB, free: 434.3 MiB)
22/04/03 15:42:23 INFO TaskSetManager: Starting task 0.0 in stage 55.0 (TID 36) (10.219.149.130, executor driver, partition 0, NODE_LOCAL, 4271 bytes) taskResourceAssignments Map()
22/04/03 15:42:23 INFO TaskSetManager: Starting task 1.0 in stage 55.0 (TID 37) (10.219.149.130, executor driver, partition 1, NODE_LOCAL, 4271 bytes) taskResourceAssignments Map()
22/04/03 15:42:23 INFO Executor: Running task 0.0 in stage 55.0 (TID 36)
22/04/03 15:42:23 INFO Executor: Running task 1.0 in stage 55.0 (TID 37)
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Getting 2 (897.0 B) non-empty blocks including 2 (897.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Getting 2 (897.0 B) non-empty blocks including 2 (897.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:23 INFO BlockManagerInfo: Removed broadcast_16_piece0 on 10.219.149.130:50047 in memory (size: 2.9 KiB, free: 434.3 MiB)
22/04/03 15:42:23 INFO Executor: Finished task 1.0 in stage 55.0 (TID 37). 1988 bytes result sent to driver
22/04/03 15:42:23 INFO BlockManagerInfo: Removed broadcast_15_piece0 on 10.219.149.130:50047 in memory (size: 2.7 KiB, free: 434.3 MiB)
22/04/03 15:42:23 INFO TaskSetManager: Finished task 1.0 in stage 55.0 (TID 37) in 7 ms on 10.219.149.130 (executor driver) (1/2)
22/04/03 15:42:23 INFO Executor: Finished task 0.0 in stage 55.0 (TID 36). 2022 bytes result sent to driver
22/04/03 15:42:23 INFO TaskSetManager: Finished task 0.0 in stage 55.0 (TID 36) in 8 ms on 10.219.149.130 (executor driver) (2/2)
22/04/03 15:42:23 INFO TaskSchedulerImpl: Removed TaskSet 55.0, whose tasks have all completed, from pool
22/04/03 15:42:23 INFO DAGScheduler: ResultStage 55 (collect at Graph.scala:157) finished in 0.011 s
22/04/03 15:42:23 INFO DAGScheduler: Job 9 is finished. Cancelling potential speculative or zombie tasks for this job
22/04/03 15:42:23 INFO TaskSchedulerImpl: Killing all running tasks in stage 55: Stage finished
22/04/03 15:42:23 INFO DAGScheduler: Job 9 finished: collect at Graph.scala:157, took 0.031594 s
22/04/03 15:42:23 INFO BlockManagerInfo: Removed broadcast_3_piece0 on 10.219.149.130:50047 in memory (size: 4.9 KiB, free: 434.3 MiB)
(4,(2,CompactBuffer(6)))
(6,(3,List()))
(2,(1,CompactBuffer(3, 4)))
(1,(0,CompactBuffer(2, 3)))
(3,(1,CompactBuffer(4, 5)))
(7,(2147483647,CompactBuffer(3)))
(5,(2,CompactBuffer(1)))
22/04/03 15:42:23 INFO BlockManagerInfo: Removed broadcast_12_piece0 on 10.219.149.130:50047 in memory (size: 2.9 KiB, free: 434.3 MiB)
22/04/03 15:42:23 INFO BlockManagerInfo: Removed broadcast_1_piece0 on 10.219.149.130:50047 in memory (size: 4.1 KiB, free: 434.3 MiB)
22/04/03 15:42:23 INFO BlockManagerInfo: Removed broadcast_17_piece0 on 10.219.149.130:50047 in memory (size: 2.7 KiB, free: 434.3 MiB)
22/04/03 15:42:23 INFO BlockManagerInfo: Removed broadcast_8_piece0 on 10.219.149.130:50047 in memory (size: 4.0 KiB, free: 434.3 MiB)
22/04/03 15:42:23 INFO BlockManagerInfo: Removed broadcast_5_piece0 on 10.219.149.130:50047 in memory (size: 3.0 KiB, free: 434.3 MiB)
22/04/03 15:42:23 INFO BlockManagerInfo: Removed broadcast_2_piece0 on 10.219.149.130:50047 in memory (size: 4.5 KiB, free: 434.4 MiB)
22/04/03 15:42:23 INFO BlockManagerInfo: Removed broadcast_6_piece0 on 10.219.149.130:50047 in memory (size: 4.6 KiB, free: 434.4 MiB)
22/04/03 15:42:23 INFO BlockManagerInfo: Removed broadcast_4_piece0 on 10.219.149.130:50047 in memory (size: 4.7 KiB, free: 434.4 MiB)
22/04/03 15:42:23 INFO SparkContext: Starting job: sortByKey at Graph.scala:163
22/04/03 15:42:23 INFO DAGScheduler: Registering RDD 24 (map at Graph.scala:161) as input to shuffle 9
22/04/03 15:42:23 INFO DAGScheduler: Got job 10 (sortByKey at Graph.scala:163) with 2 output partitions
22/04/03 15:42:23 INFO DAGScheduler: Final stage: ResultStage 66 (sortByKey at Graph.scala:163)
22/04/03 15:42:23 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 65)
22/04/03 15:42:23 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 65)
22/04/03 15:42:23 INFO DAGScheduler: Submitting ShuffleMapStage 65 (MapPartitionsRDD[24] at map at Graph.scala:161), which has no missing parents
22/04/03 15:42:23 INFO MemoryStore: Block broadcast_20 stored as values in memory (estimated size 6.0 KiB, free 434.1 MiB)
22/04/03 15:42:23 INFO MemoryStore: Block broadcast_20_piece0 stored as bytes in memory (estimated size 3.4 KiB, free 434.1 MiB)
22/04/03 15:42:23 INFO BlockManagerInfo: Added broadcast_20_piece0 in memory on 10.219.149.130:50047 (size: 3.4 KiB, free: 434.4 MiB)
22/04/03 15:42:23 INFO SparkContext: Created broadcast 20 from broadcast at DAGScheduler.scala:1427
22/04/03 15:42:23 INFO DAGScheduler: Submitting 2 missing tasks from ShuffleMapStage 65 (MapPartitionsRDD[24] at map at Graph.scala:161) (first 15 tasks are for partitions Vector(0, 1))
22/04/03 15:42:23 INFO TaskSchedulerImpl: Adding task set 65.0 with 2 tasks resource profile 0
22/04/03 15:42:23 INFO TaskSetManager: Starting task 0.0 in stage 65.0 (TID 38) (10.219.149.130, executor driver, partition 0, NODE_LOCAL, 4260 bytes) taskResourceAssignments Map()
22/04/03 15:42:23 INFO TaskSetManager: Starting task 1.0 in stage 65.0 (TID 39) (10.219.149.130, executor driver, partition 1, NODE_LOCAL, 4260 bytes) taskResourceAssignments Map()
22/04/03 15:42:23 INFO Executor: Running task 1.0 in stage 65.0 (TID 39)
22/04/03 15:42:23 INFO Executor: Running task 0.0 in stage 65.0 (TID 38)
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Getting 2 (897.0 B) non-empty blocks including 2 (897.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Getting 2 (897.0 B) non-empty blocks including 2 (897.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:23 INFO Executor: Finished task 0.0 in stage 65.0 (TID 38). 1465 bytes result sent to driver
22/04/03 15:42:23 INFO Executor: Finished task 1.0 in stage 65.0 (TID 39). 1465 bytes result sent to driver
22/04/03 15:42:23 INFO TaskSetManager: Finished task 0.0 in stage 65.0 (TID 38) in 10 ms on 10.219.149.130 (executor driver) (1/2)
22/04/03 15:42:23 INFO TaskSetManager: Finished task 1.0 in stage 65.0 (TID 39) in 9 ms on 10.219.149.130 (executor driver) (2/2)
22/04/03 15:42:23 INFO TaskSchedulerImpl: Removed TaskSet 65.0, whose tasks have all completed, from pool
22/04/03 15:42:23 INFO DAGScheduler: ShuffleMapStage 65 (map at Graph.scala:161) finished in 0.016 s
22/04/03 15:42:23 INFO DAGScheduler: looking for newly runnable stages
22/04/03 15:42:23 INFO DAGScheduler: running: Set()
22/04/03 15:42:23 INFO DAGScheduler: waiting: Set(ResultStage 66)
22/04/03 15:42:23 INFO DAGScheduler: failed: Set()
22/04/03 15:42:23 INFO DAGScheduler: Submitting ResultStage 66 (MapPartitionsRDD[27] at sortByKey at Graph.scala:163), which has no missing parents
22/04/03 15:42:23 INFO MemoryStore: Block broadcast_21 stored as values in memory (estimated size 6.9 KiB, free 434.1 MiB)
22/04/03 15:42:23 INFO MemoryStore: Block broadcast_21_piece0 stored as bytes in memory (estimated size 3.7 KiB, free 434.1 MiB)
22/04/03 15:42:23 INFO BlockManagerInfo: Added broadcast_21_piece0 in memory on 10.219.149.130:50047 (size: 3.7 KiB, free: 434.4 MiB)
22/04/03 15:42:23 INFO SparkContext: Created broadcast 21 from broadcast at DAGScheduler.scala:1427
22/04/03 15:42:23 INFO DAGScheduler: Submitting 2 missing tasks from ResultStage 66 (MapPartitionsRDD[27] at sortByKey at Graph.scala:163) (first 15 tasks are for partitions Vector(0, 1))
22/04/03 15:42:23 INFO TaskSchedulerImpl: Adding task set 66.0 with 2 tasks resource profile 0
22/04/03 15:42:23 INFO TaskSetManager: Starting task 0.0 in stage 66.0 (TID 40) (10.219.149.130, executor driver, partition 0, NODE_LOCAL, 4271 bytes) taskResourceAssignments Map()
22/04/03 15:42:23 INFO TaskSetManager: Starting task 1.0 in stage 66.0 (TID 41) (10.219.149.130, executor driver, partition 1, NODE_LOCAL, 4271 bytes) taskResourceAssignments Map()
22/04/03 15:42:23 INFO Executor: Running task 1.0 in stage 66.0 (TID 41)
22/04/03 15:42:23 INFO Executor: Running task 0.0 in stage 66.0 (TID 40)
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Getting 2 (103.0 B) non-empty blocks including 2 (103.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Getting 2 (103.0 B) non-empty blocks including 2 (103.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:23 INFO Executor: Finished task 1.0 in stage 66.0 (TID 41). 1500 bytes result sent to driver
22/04/03 15:42:23 INFO TaskSetManager: Finished task 1.0 in stage 66.0 (TID 41) in 6 ms on 10.219.149.130 (executor driver) (1/2)
22/04/03 15:42:23 INFO Executor: Finished task 0.0 in stage 66.0 (TID 40). 1500 bytes result sent to driver
22/04/03 15:42:23 INFO TaskSetManager: Finished task 0.0 in stage 66.0 (TID 40) in 10 ms on 10.219.149.130 (executor driver) (2/2)
22/04/03 15:42:23 INFO TaskSchedulerImpl: Removed TaskSet 66.0, whose tasks have all completed, from pool
22/04/03 15:42:23 INFO DAGScheduler: ResultStage 66 (sortByKey at Graph.scala:163) finished in 0.013 s
22/04/03 15:42:23 INFO DAGScheduler: Job 10 is finished. Cancelling potential speculative or zombie tasks for this job
22/04/03 15:42:23 INFO TaskSchedulerImpl: Killing all running tasks in stage 66: Stage finished
22/04/03 15:42:23 INFO DAGScheduler: Job 10 finished: sortByKey at Graph.scala:163, took 0.036089 s
filterOutput
22/04/03 15:42:23 INFO SparkContext: Starting job: collect at Graph.scala:165
22/04/03 15:42:23 INFO DAGScheduler: Registering RDD 25 (reduceByKey at Graph.scala:162) as input to shuffle 10
22/04/03 15:42:23 INFO DAGScheduler: Got job 11 (collect at Graph.scala:165) with 2 output partitions
22/04/03 15:42:23 INFO DAGScheduler: Final stage: ResultStage 78 (collect at Graph.scala:165)
22/04/03 15:42:23 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 77)
22/04/03 15:42:23 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 77)
22/04/03 15:42:23 INFO DAGScheduler: Submitting ShuffleMapStage 77 (ShuffledRDD[25] at reduceByKey at Graph.scala:162), which has no missing parents
22/04/03 15:42:23 INFO MemoryStore: Block broadcast_22 stored as values in memory (estimated size 5.2 KiB, free 434.1 MiB)
22/04/03 15:42:23 INFO MemoryStore: Block broadcast_22_piece0 stored as bytes in memory (estimated size 3.2 KiB, free 434.1 MiB)
22/04/03 15:42:23 INFO BlockManagerInfo: Added broadcast_22_piece0 in memory on 10.219.149.130:50047 (size: 3.2 KiB, free: 434.4 MiB)
22/04/03 15:42:23 INFO SparkContext: Created broadcast 22 from broadcast at DAGScheduler.scala:1427
22/04/03 15:42:23 INFO DAGScheduler: Submitting 2 missing tasks from ShuffleMapStage 77 (ShuffledRDD[25] at reduceByKey at Graph.scala:162) (first 15 tasks are for partitions Vector(0, 1))
22/04/03 15:42:23 INFO TaskSchedulerImpl: Adding task set 77.0 with 2 tasks resource profile 0
22/04/03 15:42:23 INFO TaskSetManager: Starting task 0.0 in stage 77.0 (TID 42) (10.219.149.130, executor driver, partition 0, NODE_LOCAL, 4260 bytes) taskResourceAssignments Map()
22/04/03 15:42:23 INFO TaskSetManager: Starting task 1.0 in stage 77.0 (TID 43) (10.219.149.130, executor driver, partition 1, NODE_LOCAL, 4260 bytes) taskResourceAssignments Map()
22/04/03 15:42:23 INFO Executor: Running task 1.0 in stage 77.0 (TID 43)
22/04/03 15:42:23 INFO Executor: Running task 0.0 in stage 77.0 (TID 42)
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Getting 2 (103.0 B) non-empty blocks including 2 (103.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Getting 2 (103.0 B) non-empty blocks including 2 (103.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:23 INFO Executor: Finished task 1.0 in stage 77.0 (TID 43). 1422 bytes result sent to driver
22/04/03 15:42:23 INFO TaskSetManager: Finished task 1.0 in stage 77.0 (TID 43) in 11 ms on 10.219.149.130 (executor driver) (1/2)
22/04/03 15:42:23 INFO Executor: Finished task 0.0 in stage 77.0 (TID 42). 1422 bytes result sent to driver
22/04/03 15:42:23 INFO TaskSetManager: Finished task 0.0 in stage 77.0 (TID 42) in 12 ms on 10.219.149.130 (executor driver) (2/2)
22/04/03 15:42:23 INFO TaskSchedulerImpl: Removed TaskSet 77.0, whose tasks have all completed, from pool
22/04/03 15:42:23 INFO DAGScheduler: ShuffleMapStage 77 (reduceByKey at Graph.scala:162) finished in 0.016 s
22/04/03 15:42:23 INFO DAGScheduler: looking for newly runnable stages
22/04/03 15:42:23 INFO DAGScheduler: running: Set()
22/04/03 15:42:23 INFO DAGScheduler: waiting: Set(ResultStage 78)
22/04/03 15:42:23 INFO DAGScheduler: failed: Set()
22/04/03 15:42:23 INFO DAGScheduler: Submitting ResultStage 78 (ShuffledRDD[28] at sortByKey at Graph.scala:163), which has no missing parents
22/04/03 15:42:23 INFO MemoryStore: Block broadcast_23 stored as values in memory (estimated size 5.1 KiB, free 434.1 MiB)
22/04/03 15:42:23 INFO MemoryStore: Block broadcast_23_piece0 stored as bytes in memory (estimated size 3.0 KiB, free 434.1 MiB)
22/04/03 15:42:23 INFO BlockManagerInfo: Added broadcast_23_piece0 in memory on 10.219.149.130:50047 (size: 3.0 KiB, free: 434.4 MiB)
22/04/03 15:42:23 INFO SparkContext: Created broadcast 23 from broadcast at DAGScheduler.scala:1427
22/04/03 15:42:23 INFO DAGScheduler: Submitting 2 missing tasks from ResultStage 78 (ShuffledRDD[28] at sortByKey at Graph.scala:163) (first 15 tasks are for partitions Vector(0, 1))
22/04/03 15:42:23 INFO TaskSchedulerImpl: Adding task set 78.0 with 2 tasks resource profile 0
22/04/03 15:42:23 INFO TaskSetManager: Starting task 0.0 in stage 78.0 (TID 44) (10.219.149.130, executor driver, partition 0, NODE_LOCAL, 4271 bytes) taskResourceAssignments Map()
22/04/03 15:42:23 INFO TaskSetManager: Starting task 1.0 in stage 78.0 (TID 45) (10.219.149.130, executor driver, partition 1, NODE_LOCAL, 4271 bytes) taskResourceAssignments Map()
22/04/03 15:42:23 INFO Executor: Running task 0.0 in stage 78.0 (TID 44)
22/04/03 15:42:23 INFO Executor: Running task 1.0 in stage 78.0 (TID 45)
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Getting 2 (98.0 B) non-empty blocks including 2 (98.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Getting 2 (98.0 B) non-empty blocks including 2 (98.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
22/04/03 15:42:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/03 15:42:23 INFO Executor: Finished task 0.0 in stage 78.0 (TID 44). 1393 bytes result sent to driver
22/04/03 15:42:23 INFO TaskSetManager: Finished task 0.0 in stage 78.0 (TID 44) in 6 ms on 10.219.149.130 (executor driver) (1/2)
22/04/03 15:42:23 INFO Executor: Finished task 1.0 in stage 78.0 (TID 45). 1436 bytes result sent to driver
22/04/03 15:42:23 INFO TaskSetManager: Finished task 1.0 in stage 78.0 (TID 45) in 6 ms on 10.219.149.130 (executor driver) (2/2)
22/04/03 15:42:23 INFO TaskSchedulerImpl: Removed TaskSet 78.0, whose tasks have all completed, from pool
22/04/03 15:42:23 INFO DAGScheduler: ResultStage 78 (collect at Graph.scala:165) finished in 0.011 s
22/04/03 15:42:23 INFO DAGScheduler: Job 11 is finished. Cancelling potential speculative or zombie tasks for this job
22/04/03 15:42:23 INFO TaskSchedulerImpl: Killing all running tasks in stage 78: Stage finished
22/04/03 15:42:23 INFO DAGScheduler: Job 11 finished: collect at Graph.scala:165, took 0.029229 s
(0,1)
(1,2)
(2,2)
(3,1)
22/04/03 15:42:23 INFO SparkContext: Invoking stop() from shutdown hook
22/04/03 15:42:23 INFO SparkUI: Stopped Spark web UI at http://10.219.149.130:4040
22/04/03 15:42:23 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
22/04/03 15:42:23 INFO MemoryStore: MemoryStore cleared
22/04/03 15:42:23 INFO BlockManager: BlockManager stopped
22/04/03 15:42:23 INFO BlockManagerMaster: BlockManagerMaster stopped
22/04/03 15:42:23 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
22/04/03 15:42:23 INFO SparkContext: Successfully stopped SparkContext
22/04/03 15:42:23 INFO ShutdownHookManager: Shutdown hook called
22/04/03 15:42:23 INFO ShutdownHookManager: Deleting directory /private/var/folders/x8/lld34qc52sj2p7ssk9ylpp5r0000gn/T/spark-5e91e666-98cd-45d8-b081-5966ad1461e9
22/04/03 15:42:23 INFO ShutdownHookManager: Deleting directory /private/var/folders/x8/lld34qc52sj2p7ssk9ylpp5r0000gn/T/spark-17fa2ae8-7821-40c8-835b-5219752087c3
amlanalok@Amlans-MacBook-Pro Learning-Spark %