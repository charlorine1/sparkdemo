package com.usst.spark.action;

import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

public class Operator_foreachPartition {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("foreachPartition");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile("./data/words.txt",3);
		lines.foreachPartition(new VoidFunction<Iterator<String>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = -2302401945670821407L;

			@Override
			public void call(Iterator<String> t) throws Exception {
				System.out.println("创建数据库连接。。。");
				while(t.hasNext()){
					System.out.println(t.next());
				}
			}
		});
		sc.stop();
	}

}

/**
=================================================================================
19/05/13 18:43:35 INFO deprecation: mapred.task.partition is deprecated. Instead, use mapreduce.task.partition
19/05/13 18:43:35 INFO deprecation: mapred.job.id is deprecated. Instead, use mapreduce.job.id
创建数据库连接。。。
hello wordl
hello usst

19/05/13 18:43:35 INFO Executor: Finished task 0.0 in stage 0.0 (TID 0). 2044 bytes result sent to driver
19/05/13 18:43:35 INFO TaskSetManager: Starting task 1.0 in stage 0.0 (TID 1, localhost, partition 1,PROCESS_LOCAL, 2062 bytes)
19/05/13 18:43:35 INFO Executor: Running task 1.0 in stage 0.0 (TID 1)
19/05/13 18:43:35 INFO HadoopRDD: Input split: file:/E:/xiangmu/sparkdemo/data/words.txt:18+18
19/05/13 18:43:35 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 59 ms on localhost (1/3)
创建数据库连接。。。
hello haha

19/05/13 18:43:35 INFO Executor: Finished task 1.0 in stage 0.0 (TID 1). 2044 bytes result sent to driver
19/05/13 18:43:35 INFO TaskSetManager: Starting task 2.0 in stage 0.0 (TID 2, localhost, partition 2,PROCESS_LOCAL, 2062 bytes)
19/05/13 18:43:35 INFO TaskSetManager: Finished task 1.0 in stage 0.0 (TID 1) in 10 ms on localhost (2/3)
19/05/13 18:43:35 INFO Executor: Running task 2.0 in stage 0.0 (TID 2)
19/05/13 18:43:35 INFO HadoopRDD: Input split: file:/E:/xiangmu/sparkdemo/data/words.txt:36+18
创建数据库连接。。。
hello usst
aa ab


19/05/13 18:43:35 INFO Executor: Finished task 2.0 in stage 0.0 (TID 2). 2044 bytes result sent to driver
19/05/13 18:43:35 INFO TaskSetManager: Finished task 2.0 in stage 0.0 (TID 2) in 12 ms on localhost (3/3)
=================================================================================
**/
