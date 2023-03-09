1. RDD 是什么？  
是一个弹性分布式数据集，具备以下五个特征：  
A. 可分区的: 每一个分区对应就是一个Task线程  ```a list of partitions```
B. 计算函数(对每个分区进行计算操作) ```a function for computing each split``` 
C. 存在依赖关系(提供容错机制) ```a list of dependencies on other RDDs```
D. 对于key-value数据存在分区计算函数 ```a Partitioner for key-value RDDs```  
E. 移动数据不如移动计算(将计算程序运行在离数据越近越好) ```a list of preferred locations to compute each spl  it```  

2. LineAge 是什么？ 
3. pipeline 是什么？  
4. RDD 间的依赖关系有哪些？  
A. Dependency   
B. NarrowDependency  
C. ShuffleDependency 宽依赖(一对多)  
D. OneToOneDependency 窄依赖(一对一)  
E. RangeDependency 窄依赖(一对一，多对一)  
关系图：A->B->D, A->B->E, A->C
5. 