首先是简单的engine-crush-test.py。是每一个OSD对应一个Engine，这样就保证了相同id的Engine和OSD肯定在同一个节点中。

## engine-crush-test.py

首先是简单的engine-crush-test.py。是每一个OSD对应一个Engine，这样就保证了相同id的Engine和OSD肯定在同一个节点中。

### 实现上是：

1. 先将Object映射到engine（fill_engine），采用的是ceph里的ceph_str_hash_rjenkins哈希映射。
2. PG映射到OSD(fill_osd_with_pg)，采用的是CRUSH映射。这里每个PG映射到的第一个OSD是主OSD。记录下每个OSD中以该OSD作为主OSD的PG有哪些（pg_pri_in_osd），每个OSD中以该OSD作为从OSD的PG有哪些（pg_rep_in_osd）。
3. engine到PG映射(engine_to_pg_naive_hash, engine_to_pg_crush)。**其实就是对于每个engine中的Object，在以该engine所在节点中的osd为主osd的PG中选择一个PG**。但由于目前只是简单的每一个OSD对应一个Engine，所以直接在该engine对应的osd的pg_pri_in_osd中选择一个就好了。(engine_to_pg_naive_hash中obj选择pg时是ceph_str_hash_rjenkins哈希映射。engine_to_pg_crush中obj选择pg时是副本数为1的CRUSH。



## engine-crush-plus.py

然后是engine-crush-plus.py。不再每一个OSD对应一个Engine了。每个节点一般设置8个engine。而OSD数可能是几十个。

因此**需要记录每个节点有哪些OSD，有哪些Engine**。并且**在第二步的时候记录的是每个节点中以该节点中的OSD作为主OSD的PG有哪些**，然后第三步中**对于每个engine中的Object，在以该engine所在节点中的osd为主osd的PG中选择一个PG**。

### 实现上前两步基本上不变：

1. 先将Object映射到engine（fill_engine），采用的是ceph里的ceph_str_hash_rjenkins哈希映射。
2. PG映射到OSD(fill_osd_with_pg)，采用的是CRUSH映射。这里每个PG映射到的第一个OSD是主OSD。记录下**每个节点中以该节点中的OSD作为主OSD的PG有哪些**（pg_pri_in_host），**每个节点中以该节点中的OSD作为从OSD的PG有哪些**（pg_rep_in_host）。
3. engine到PG映射(engine_to_pg_naive_hash, engine_to_pg_crush)。**其实就是对于每个engine中的Object，在以该engine所在节点中的osd为主osd的PG中选择一个PG**。首先找到这个Engine在哪个host中，然后该engine中的Object在该host的pg_pri_in_host中选择pg去映射。最后从pg映射到osd，在仿真中不记录每个osd中有哪些Object，存储时只增加每个osd的使用率。(engine_to_pg_naive_hash中obj选择pg时是ceph_str_hash_rjenkins哈希映射。engine_to_pg_crush中obj选择pg时是副本数为1的CRUSH。



**总结下来其实就是把Object和PG都按照host分组了**。每个host中有一些Object，有一些PG以该host中的OSD作为主OSD，然后这些Object只能映射到这些PG中。

所以引入weight hash时，由于每个host包含的PG的总权重不同，每个host中的Object数也应该不同。**每个host中的Object数应该与PG总权重成正比。**

所以引入weight hash时，不仅要在object从engine映射到PG时进行加权哈希（因为每个PG的权重不同），还要在Object映射到Engine时就要加权哈希，**对于每个host设置一个权重，设置为该host包含的PG的总权重，然后同一个host中的engine权重均等于host权重。**