# WeightedPG

新华三华科联合实验室数据打散项目。

在Ceph系统中实现：https://github.com/EdgarGong/Ceph-weightedPG。
[python-crush](https://github.com/ceph/python-crush)

使用[python-crush](https://github.com/ceph/python-crush)环境。

所有项目代码位于`mytests`文件夹下，在`mytests`文件夹下运行。

python-crush C语言部分编译方法：

```shell
\# 在根目录下

rm -rf ./build

python ./setup.py install  # python为python2
```



### WeightedPG

项目main idea，即基于`Ceph`的加权哈希映射方案实现。

位于`WeightedPG.py`，程序执行：

```shell
python WeightedPG.py
```



将会自动对于不同`主机数、每个主机硬盘数、冗余策略、副本数（或k、m）`参数。

对于每个参数，程序将：

- 初始化Balancer，并计算存储object数量。
- 使用CRUSH算法将PG映射至OSD。
- 根据PG到OSD映射结果计算各个PG的Rendezvous hashing权重。
- 存储object。对于每个object：
  1. 使用加权哈希映射选择PG。
  2. 计算该PG映射至的若干OSD，并存入OSD。
- 计算每个OSD的均衡度。即计算平均OSD存储容量，并计算最大/最小OSD存储容量与平均OSD存储容量的比值。



### engine test

即基于engine存储系统的加权哈希映射方案实现。

位于`engine-test.py`，程序执行：

```shell
python engine-test.py
```

程序将：

- 初始化Balancer，并计算存储object数量。
- 使用CRUSH算法将PG映射至OSD。
- 根据PG到OSD映射结果计算各个PG的Rendezvous hashing权重。并计算每个host的以该host中的OSD作为主OSD的所有PG的权重之和，并将该host中的engine的权重值均设为该权重和。
- 使用加权哈希映射将object映射至engine。
- 对于每个engine分别存储映射到其中的object。对于每个object：
  1. 使用加权哈希映射在该engine所在的host的以该host中的OSD作为主OSD的所有PG中选择PG。
  2. 计算该PG映射至的若干OSD，并存入OSD。
- 计算每个OSD的均衡度。即计算平均OSD存储容量，并计算最大/最小OSD存储容量与平均OSD存储容量的比值。



### scaling test

项目扩容实验。

位于`scaling-test.py`，程序执行：

```shell
python scaling-test.py
```



将会自动对于不同`主机数、每个主机硬盘数、冗余策略、副本数（或k、m）`参数。

对于每个参数，程序将：

- 分别初始化扩容前和扩容后的集群规模对应的两个Balancer，并计算存储object数量。
- 对于扩容前和扩容后，分别使用CRUSH算法将PG映射至OSD。
- 分别根据PG到OSD映射结果计算各个PG的Rendezvous hashing权重。
- 首先使用传统Ceph中的传统哈希映射，对扩容前和扩容后的集群分别模拟存入object，计算扩容前后object迁移量，来作为与WeightedPG方案的对比，即扩容前后集群映射到的OSD不同的object数量。
- 然后使用WeightedPG方案加权哈希映射。将进行：
  - 对扩容前和扩容后的集群分别模拟存入object，计算扩容前后object迁移量，即扩容前后集群映射到的OSD不同的object数量。扩容前后object均使用扩容前旧集群中的PG权重值。并计算扩容前和扩容后的集群OSD均衡度。
  - 根据扩容后此时的各个OSD剩余容量，将其作为OSD权重，来计算新的PG权重值。
  - 此时再存入object，使得集群总存储百分比达到扩容前水平。对于新存入的Objecter，使用扩容后新计算的PG权重值。
  - 计算再存入后的集群OSD均衡度。





### failure test

项目故障修复实验。

位于`failure -test.py`，程序执行：

```shell
python failure-test.py
```



将会自动对于不同`主机数、每个主机硬盘数、冗余策略、副本数（或k、m）`参数。

对于每个参数，程序将：

- 分别初始化故障前和故障后的集群规模对应的两个Balancer，并计算存储object数量。
- 对于故障前和故障后，分别使用CRUSH算法将PG映射至OSD。
- 分别根据PG到OSD映射结果计算各个PG的Rendezvous hashing权重。
- 首先使用传统Ceph中的传统哈希映射，对故障前和故障后的集群分别模拟存入object，计算故障前后object迁移量，来作为与WeightedPG方案的对比，即故障前后集群映射到的OSD不同的object数量。
- 然后使用WeightedPG方案加权哈希映射。将进行：
  - 对故障前和故障后的集群分别模拟存入object，计算故障前后object迁移量，即故障前后集群映射到的OSD不同的object数量。故障前后object均使用故障前旧集群中的PG权重值。并计算故障前和故障后的集群OSD均衡度。
