package com.usst.spark.transformations;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
/**
 * union  后的分区数是unionRDD分区的总和
 * @author root
 *
 */
public class Operator_union {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("union");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1,2,3),2);
		JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(4,5,6),1);
		JavaRDD<Integer> union = rdd1.union(rdd2);
		System.out.println("union.partitions().size()---"+union.partitions().size());
		union.foreach(new VoidFunction<Integer>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer t) throws Exception {
				System.out.println(t);
			}
		});
		
		sc.stop();
	}
}


/**
 * union.partitions().size()---3
=================================================================================
19/05/13 18:01:20 INFO TaskSchedulerImpl: Adding task set 0.0 with 3 tasks
19/05/13 18:01:20 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0, localhost, partition 0,PROCESS_LOCAL, 2158 bytes)
19/05/13 18:01:20 INFO Executor: Running task 0.0 in stage 0.0 (TID 0)
1
19/05/13 18:01:20 INFO Executor: Finished task 0.0 in stage 0.0 (TID 0). 915 bytes result sent to driver
19/05/13 18:01:20 INFO TaskSetManager: Starting task 1.0 in stage 0.0 (TID 1, localhost, partition 1,PROCESS_LOCAL, 2168 bytes)
19/05/13 18:01:20 INFO Executor: Running task 1.0 in stage 0.0 (TID 1)
2
3
19/05/13 18:01:20 INFO Executor: Finished task 1.0 in stage 0.0 (TID 1). 915 bytes result sent to driver
19/05/13 18:01:20 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 35 ms on localhost (1/3)
19/05/13 18:01:20 INFO TaskSetManager: Starting task 2.0 in stage 0.0 (TID 2, localhost, partition 2,PROCESS_LOCAL, 2178 bytes)
19/05/13 18:01:20 INFO Executor: Running task 2.0 in stage 0.0 (TID 2)
19/05/13 18:01:20 INFO TaskSetManager: Finished task 1.0 in stage 0.0 (TID 1) in 6 ms on localhost (2/3)
4
5
6
19/05/13 18:01:20 INFO Executor: Finished task 2.0 in stage 0.0 (TID 2). 915 bytes result sent to driver
=================================================================================
**/