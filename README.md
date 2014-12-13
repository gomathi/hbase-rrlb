hbase-rrlb
==========

An implementation of hbase load balancer which colocates related regions together. 

Most Hbase based applications have data in multiple tables for a given user. Usually the tables are divided into fixed no of regions based on the user ids or application specific logic. It is a good practice that placing all related regions in same region server. This will avoid unnecessary network hops.

This load balancer provides following support

1. Colocating related regions. There is a way to inform which tables are related tables to the load balancer, and based on that the placement is executed by the load balancer.
2. Weighted load balancing. This allows more heavier hosts to get more no of regions. There is a way to provide hostweights. By default all the region servers are taking equal weight.
3. When HBase load balancer returns the region plans, the assignment manager immediately sends out the regions for movements. This will cause huge no of movements of regions. This load balancer provides a way to move only fixed no of regions at a time. You can enable/disable it when you create the load balancer.

To use this package, you need to do following things

1. You need to extend the loadbalancer class, and from the child class you need to provide information about which tables are related to base load balancer. Also a hostweights conf file, and directory location. Also you should mention whether the load balancer has to do less aggressive region placements.
2. You need to generate the jar file. This pacakge is tested against Hbase 94 and Hbase 98. 
3. Put the jar file in the hbase master server's classpath. 
4. Add following configurations in hbase-site.xml

```xml
<property>
        <name>hbase.master.loadbalancer.class</name>
        <value>org.balancer.custombalancer.CustomLoadBalancer</value>
</property>
```

and 

```xml
<property>
        <name>hbase.master.loadbalance.bytable</name>
        <value>false</value>
</property>
```

Look at https://github.com/gomathi/hbase-rrlb/blob/master/src/org/balancer/custombalancer/CustomLoadBalancer.java for a child class example.

Look at the main load balancer class at https://github.com/gomathi/hbase-rrlb/blob/master/src/org/balancer/rrlbalancer/RelatedRegionsLoadBalancer.java

Further description is as below
-

I am working on a project that uses HBase. The project has multiple tables. Each table entry is uniquely identified by a id. HBase supports high write throughput as well as high random read throughput. But that does not mean, you can just drop everything in HBase, and you can still achieve a reduced latency for your application. You have to tune lot of parameters and probably plugin your own modules into HBase to achieve what you want for your application. And that's the fun part.

Problem
-

We have pre-divided the regions for each table. A set of tables have same no of regions, and each region share same start key and end key. For a given user, data is available on all these tables, and the regions of the user will have same start key and end key. When I was reading HBase and Hadoop, the best practice suggests to colocate data in the same server. Thus, for any query, and for a given user, HBase does not have to talk to multiple region servers. If you can place all related regions in a single region server, then overall we can achieve reduced latency.  Unfortunately HBase does not provide any load balancer LoadBalancer (HBase 2.0.0-SNAPSHOT API) that supports placing related regions in a region server. But its easy to write your own load balancer implementation and plug it in. 

Solution
-

I wrote a load balancer which groups regions from different tables if they share same start key and end key. For example we have 3 tables (t1, t2, t3), and each table has 3 regions with start key and end key as following (1,10), (11,20), (21,30).  If you cluster these tables based on just start key and end key, you will get 

[ t1(1,10), t2((1,10), t3(1,10)] 
[ t1(11,20), t2((11,20), t3(11,20)] 
[ t1(21,30), t2((21,30), t3(21,30)]

Main function of this load balancer is to group related regions and try to place all related regions on one region server. Also I provided the control to the application to define which tables are related tables. Thus we can have fine grained control about which regions should be placed together. 

Default Load Balancer - Algorithm - HBase

The default load balancer(HBase) has an easier algorithm implementation to load balance regions across available region servers. The algorithm works as following

1) Calculate the total no of regions, and find the average no of regions per region server by dividing the total no of regions by available servers count. Since total may not be evenly divisible, its necessary that we calculate min and max as per the average value. For example if there are 8 regions, and there are 3 servers, then the average is 2. Minimum value is 2, and maximum value is (min + 1) if totalRegions%totServers != 0. Here it is 3, as 8 % 3 => 2.

2) Sort all servers by their load, and make sure the sorted sequence is non decreasing. 

3) If first and last server's load are within the interval >= min and <= max, then load is balanced. 

4) Otherwise, first reduce the load on servers which have > max no of regions, by placing the load on servers which have < max load. Once this is done, all servers will have <= max regions. But there is no guarantee that all servers will have >= min regions. So its necessary that we run one more step, where we take regions from servers which have > min regions, and place them into servers which have < min regions. 

The algorithm is so simple. It also does not use any modulo hashing/consistent hashing techniques, and it avoids unnecessary swapping of regions. Moving regions between servers involves the regions to be not available for shorter period of time. We should avoid swapping regions unnecessarily.

Colocating Regions
-

I just extended above mentioned algorithm to group related regions. By default, HBase calls load balancer with regions for each table. It does not combine all regions from all tables, and it does not pass those values to the load balancer.

hbase.master.loadbalance.bytable = false

Once you get all the regions, the algorithm clusters them based on related tables information. Now clusters are distributed across all region servers. When a region server goes down, this load balancer gets the call. RandomAssignment function is called. It is necessary that we should try to place all related regions into same region server during the random assignment. Otherwise the reads/writes to the related regions might provide increased latency. With Hbase 98, randomAssignment API is defined as randomAssignment(HRegion region, List<ServerName> servers). So we can use the HRegion information, and store which server is assigned in memory. So when a call comes for next related region, we can simply return the previously assigned region server. This way we can always make sure all the related regions are colocated. 

Weighted balancing
-

In production, you can not expect all region servers of same configuration, and same capability. Mostly the environment is heterogeneous. So a heavier host can get more regions than lighter host. I extended the above algorithm to support weighted balancing. I made few mistakes along the way, and fixed them.

When I started thinking about solving the problem, I did not want to change the load balancer algorithm, rather just wanted to wrap around with another method, which introduces some intelligence about weighted load balancing. By default all servers will carry a weight of 1. If the weight is more than 1, and then I add virtual servers to the set of available servers, and I pass it to the load balancer. So the core algorithm just runs load balancing, and returns the region plans. Since I know the actual server to virtual server mapping, I replace the virtual server to actual server, and return the region plans.

Method of adding virtual servers to do weighted load balancing actually resulted in expected result. But it caused unnecessary swapping of regions. For example, we have two servers A and B, and each have 4 regions. Each host carries a weight of 4. 

A [1,2,3,4]
B [5,6,7,8]

When the algorithm runs, it adds 6 virtual servers, since each host carry weight of 4, and already we have 2 physical servers. Load balancer will get the following server and region mappings. 

A[1,2,3,4]
B [5,6,7,8]
V1[]
V2[]
V3[]
V4[]
V5[]
V6[]

Virtual server to Physical Server mapping

V1 -> A
V2 -> A
V3 -> A
V4 -> B
V5 -> B
V6 -> B

Even though actual physical servers are already balanced as per their weight, as we are adding virtual servers, it might cause unnecessary swapping of regions.

For example, the algorithm may result in the following result

A [1,2,5,6]
B[5,6,3,4]

I avoided this unnecessary swapping by distributing regions from the physical server to virtual server before adding the virtual server to the server -> region mappings. 

This changes just work fine so far. I am extending this idea to load balance based on the cpu usage over a period of time. 
