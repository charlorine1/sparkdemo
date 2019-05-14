package com.usst.spark.transformations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;


/**
 * 
 * mapPartitionsWithIndex 方法的参数是<Integer, Iterator<String>, Iterator<String>>
 *                                   分区值          入参 的集合                    出参也是一个list
 * 
 * 
 * 
 * */
public class Operator_mapPartitionWithIndex {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("mapPartitionWithIndex");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<String> names = Arrays.asList("zhangsan1", "zhangsan2", "zhangsan3","zhangsan4");
		
		/**
		 * 这里的第二个参数是设置并行度,也是RDD的分区数，并行度理论上来说设置大小为core的2~3倍
		 */
		JavaRDD<String> parallelize = sc.parallelize(names, 3);
		JavaRDD<String> mapPartitionsWithIndex = parallelize.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(Integer index, Iterator<String> iter)
					throws Exception {
				List<String> list = new ArrayList<String>();
				while(iter.hasNext()){
					String s = iter.next();
					list.add(s+"~");
					System.out.println("partition id is "+index +",value is "+s );
				}
				return list.iterator();
			}
		}, true);
		mapPartitionsWithIndex.collect();
		sc.stop();
	}

}



/**
=================================================================================
partition id is 0,value is zhangsan1
19/05/13 16:36:57 INFO Executor: Finished task 0.0 in stage 0.0 (TID 0). 928 bytes result sent to driver
19/05/13 16:36:57 INFO TaskSetManager: Starting task 1.0 in stage 0.0 (TID 1, localhost, partition 1,PROCESS_LOCAL, 2011 bytes)
19/05/13 16:36:57 INFO Executor: Running task 1.0 in stage 0.0 (TID 1)

partition id is 1,value is zhangsan2
19/05/13 16:36:57 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 35 ms on localhost (1/3)
19/05/13 16:36:57 INFO Executor: Finished task 1.0 in stage 0.0 (TID 1). 928 bytes result sent to driver
19/05/13 16:36:57 INFO TaskSetManager: Starting task 2.0 in stage 0.0 (TID 2, localhost, partition 2,PROCESS_LOCAL, 2023 bytes)
19/05/13 16:36:57 INFO Executor: Running task 2.0 in stage 0.0 (TID 2)
19/05/13 16:36:57 INFO TaskSetManager: Finished task 1.0 in stage 0.0 (TID 1) in 5 ms on localhost (2/3)

partition id is 2,value is zhangsan3
partition id is 2,value is zhangsan4
19/05/13 16:36:57 INFO Executor: Finished task 2.0 in stage 0.0 (TID 2). 941 bytes result sent to driver
19/05/13 16:36:57 INFO TaskSetManager: Finished task 2.0 in stage 0.0 (TID 2) in 4 ms on localhost (3/3)
=================================================================================
**/
