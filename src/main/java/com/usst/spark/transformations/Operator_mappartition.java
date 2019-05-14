package com.usst.spark.transformations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * mappartition 算子执行call，执行的是一个partition里面的所有集合， 
  * 一次 call 入参是partition的所有数据   call(Iterator<String> t)
 * 
 * */
public class Operator_mappartition {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("mappartition");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		//JavaRDD<String> lines = sc.textFile("./data/records.txt");
	
		List<String> list = Arrays.asList(
				"love1","love2","love3",
				"love4","love5","love6",
				"love7","love8","love9",
				"love10","love11","love12","love10"
				);
		
		JavaRDD<String> lines = sc.parallelize(list,2);
		JavaRDD<String> mapPartitions = lines.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(Iterator<String> t) throws Exception {
				
				System.out.println("创建数据库连接。。。。");
				List<String> list  = new ArrayList<>();
				while(t.hasNext()) {
					list.add(t.next());
				}
				return list;
			}
		});
		//mapPartitions.collect();
		mapPartitions.foreach(new VoidFunction<String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(String t) throws Exception {
				System.out.println(t);
				
			}
		});
		sc.stop();

	}

}

/**
 * 只执行了一次数据库连接可以看出call的入参就是该partition的所有数据
=================================================================================
				创建数据库连接。。。。
				love1
				love2
				love3
				love4
				love5
				love6
				
				19/05/13 16:33:33 INFO Executor: Finished task 0.0 in stage 0.0 (TID 0). 915 bytes result sent to driver
				19/05/13 16:33:33 INFO TaskSetManager: Starting task 1.0 in stage 0.0 (TID 1, localhost, partition 1,PROCESS_LOCAL, 2055 bytes)
				19/05/13 16:33:33 INFO Executor: Running task 1.0 in stage 0.0 (TID 1)
				创建数据库连接。。。。
				love7
				love8
				love9
				love10
				love11
				love12
				love10
=================================================================================
**/
