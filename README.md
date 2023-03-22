# Spark 
https://spark.apache.org/

## 介绍
1) 运行速度快：使用有向无环图（DAG）执行引擎
2) 容易使用： 支持Java、Scala、Python、R
3) 通用性：包括SQL 查询、流计算、机器学习、图算法组件
4) 运行模式多样性： 
   1) 本地模式：
       1) local：只启动一个executor
       2) local[k]： 启动k个execuutor
       3) local[*]：启动与cpu数目相同的executor
   2) standalone 模式: 分布式部署集群。自带完成服务，资源管理和任务监控
   3) on yarn : 分布式部署集群，资源和任务交给yarn 进行管理，粗力度资源分配方式，包含cluster 和 client 运行模式：
       1) cluster 适合生产，driver 运行在集群子节点，具有容错功能，
       2) client 适合调试， driver 运行在客户端。
   4) in Mesos: mesos 是一种资源调度框架，spark 在 mesos 上运行要比yarn 更加灵活。

## Spark 生态
1) spark core:  包含spark最基础和最核心功能，如内存计算,任务调度，部署模式，故障恢复，存储管理等，主要面向批数据处理。
2) spark sql：用于结构化数据处理组件，允许开发人员直接处理RDD，同时可以查询hive、hbase 中数据，直接使用sql 进行查询即可。
3) spark Streaming: 流计算框架，，可以支持高吞吐量，可容错处理的实时流数据处理。
4) spark MLlib：提供了常用的机器学习算法的实现
5) spark GraphX：用于图计算提供API

## Spark 运行框架
1) RDD: 弹性分布式数据集，是分布式内存的一个抽象概念，提供了一种高度受限的共享内存模型。
2) DAG: 是有向无环图，反应RDD之间依赖关系。
3) Executor：运行在节点上的一个进程，负责运行任务，并为应用程序存储数据。
4) Application： 用户编写的 Spark 应用程序。
5) Task：运行在Executor 上的工作单元。
6) Job：一个作业包含多个RDD 及 作用于 相应的RDD上的各种操作。
7) Stage：作业的基本调度单位。一个作业会分为多组任务，每组任务会被称为"阶段"或"任务集"

## Spark 运行基本流程
1) 构建Spark Application的运行环境(启动SparkContext)，SparkContext向资源管理器(YARN)注册并申请运行Executor资源；
2) 资源管理器分配并启动Executor，Executor的运行情况将随着心跳发送到资源管理器上；
3) SparkContext构建成DAG图，将DAG图分解成Stage(TaskSet)，并把TaskSet发送给Task Scheduler。Executor向SparkContext申请Task；
4) Task Scheduler 将Task发放给Executor运行，同时SparkContext将应用程序代码发放给Executor；
5) Task在Executor上运行，运行完毕释放所有资源。

## RDD 概念
是一个弹性分布式数据集，具备以下五个特征：  
1) 可分区的: 每一个分区对应就是一个Task线程  ```a list of partitions```
2) 计算函数(对每个分区进行计算操作) ```a function for computing each split```
3) 存在依赖关系(提供容错机制) ```a list of dependencies on other RDDs```
4) 对于key-value数据存在分区计算函数 ```a Partitioner for key-value RDDs```  
5) 移动数据不如移动计算(将计算程序运行在离数据越近越好) ```a list of preferred locations to compute each spl  it```

### RDD 依赖关系有哪些？
1) 窄依赖：无shuffle 操作
   1) Dependency-> NarrowDependency->OneToOneDependency (一对一)
   2) Dependency-> NarrowDependency->RangeDependency (一对一，多对一)
2) 宽依赖：有shuffle 操作
   1) Dependency-> ShuffleDependency (一对多)

## Spark-submit 运行程序
spark submit
--class <main-class> 包名+类名
--master yarn
--jar <application jar> 应用程序jar
-- executor-cores (每一个executor 使用的内核数，默认为1，建议2-5个，一般4个即可)
-- num-executors (启动executor 数目，默认为2)
-- driver-cores (driver 使用内核数，默认为1)
-- driver-memory (driver 内存大小， 默认为 512M)

## Spark on yarn 提交作业流程
### client 端
在 client 模式下，Driver 在任务提交的本地机器上运行，Driver 启动后会和 Resource Manager 通讯申请启动 Application Master，
随后 Resource Master 分配 container 在合适的 Node Manager 上启动 Application Master， 
此时的 Application Master 的功能相当于一个 executor Launcher，只负责向 Resource Manager 申请 Executor 内存。
Resource Manager 接到 Application Master 的资源申请后会分配 container， 
然后 Application Master 在资源分配指定的 Node Manager 上启动 Executor 进程， Executor 进程启动后会反向注册，当Executor 全部注册完成后，
Driver 开始执行main 函数，之后执行到 Action 算子时，触发一个job 操作，并根据宽依赖开始划分Stage，每个stage 生成对应的taskSet 
之后将task分发的各个 Executor 上执行。


### cluster 
在cluster 模式下，任务提交后会和 ResourceManager 通讯，申请启动Application Master，随后Resource Manager
分配 container，在合适的Node Manager 上启动 Application Master，此时的Application Master 就是driver。
driver 启动后会向 Resource Manager 申请 Executor 内存，Resource Manager  接到 Application Master 的资源申请后会分配 container，
然后在合适的 Node Manager 上启动 Executor， Executor 启动后会向driver 反向注册，Executor 全部注册完成后，Driver 开始执行 main 函数，
之后执行到 action 算子，触发一个job，并根据宽依赖划分stage，每个stage 生成对应 taskSet，之后将task分发到各个 executor 上进行执行。



## RDD 基础
### 转换算子
1) 不导致shuffle 
   1) map
   2) flatMap
   3) filter
2) 导致shuffle
   1) reduceByKey
   2) groupByKey
   3) combineBykey

### 执行算子
1) count()
2) collect()
3) first()
4) take(n)
5) foreach(func)
6) reduce(func)

### 惰性机制
指的是在整个转换过程中只是记录转换轨迹，并不会发生真正的计算，只有在遇到执行算子时，才会触发从头到尾的真正计算。

### 持久化
因为使用惰性机制导致每次遇到执行算子都会从头开始计算，这样对于的呆计算而言，代价是非常大的。
持久化（缓存）机制来避免这种重复计算的开销。
1) persist() 
   1) persist(memory_only) 只在内存中存储，如果内存不足，会按照LRU原则进行替换缓存中的内容
   2) persist(memory_and_disk) 如果内存不足，超出的部分会存放在磁盘中
2) cache() = persist(memory_only)
3) checkpoint
  注意： persist() 和 cache() 程序结束后会被清除或者手动调用 unpersist() 移除。
   checkpoint() 会永久保存。

### 设置分区个数
1) 创建RDD时手动设置分区数量
   1) textFile(path, 2)
   2) parallelize(Array(1,2,3,4), 2)
2) 使用 repartition 手动设置分区数量
   1) repartition 底层调用的就是 Coalesce(num, shuffle=true) 
   2) repartition 和 Coalesce 的区别就是 repartition一定会触发shuffle ，Coalesce 根据传入参数判断是否触发shuffle
   所以一般增加分区一般使用 repartition ，减少分区一般使用 coalesce。

### 共享变量
累加器（Accumulators）与广播变量（Broadcast Variables）共同作为Spark提供的两大共享变量，
主要用于跨集群的数据节点之间的数据共享，突破数据在集群各个executor不能共享问题。
1) 累加器(accumulator)：是spark 提供的一种分布式的变量机制，分别是LongAccumulator(参数支持Integer、Long)、DoubleAccumulator(参数支持Float、Double)、CollectionAccumulator(参数支持任意类型)
   累加器常见的用途是在调试时对作业执行过程中的事件进行计数。
2) 广播变量(broadcast)：每一个机器上都缓存一份，不可变，只读的相同变量的，
   该节点的每个任务都能访问的，起到节省资源并优化的作用，通常用来高效的分发较大对象。

### Spark 数据库连接数
1) 如何降低数据库连接个数？使用foreach partition 代替 foreach，在foreach Partition 中获取数据库连接，从而减少连接次数

## SparkSQL
### DataFrame

## Spark 调优
### 资源调优
1) num_executors: 设置spark作业总共需要多少个Executor 进行执行
2) executor_memory: 设置每个Executor进程内存
3) executor_cores: 设置每个Executor 进程的 CPU core 数量
4) driver_memory： 设置driver 进程内存
5) spark.default.parallelism: 设置每个stage 的默认task 数目

### 开发优化
1) 避免创建重复RDD
2) 尽可能复用同一个RDD
3) 对多次使用的RDD 进行持久化。
4) 尽量避免使用shuffle 算子
5) 使用 map-side 预聚合的 shuffle 操作
6) 使用高性能算子
7) 使用 reduceByKey/ aggregateByKey 代替 groupByKey
8) 使用 foreach Partition 代替 foreach
9) 使用filter 后进行 coalesce 操作
10) 使用 repartitionAndSortWithinPartitions 代替 repartition 与sort 操作