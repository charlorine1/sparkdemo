package com.usst.spark.transformations;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * sortByKey() 根据key值进行排序
 *     ture是升序    默认
 *     false 是降序
 * 
 * */
public class Operator_sortByKey {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("sortByKey");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<String> lines = jsc.textFile("./data/words.txt");
		JavaRDD<String> flatMap = lines.flatMap(new FlatMapFunction<String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String t) throws Exception {
				return Arrays.asList(t.split(" "));
			}
		});
		JavaPairRDD<String, Integer> mapToPair = flatMap.mapToPair(new PairFunction<String, String, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String s) throws Exception {
				return new Tuple2<String, Integer>(s, 1);
			}
		});
		
		JavaPairRDD<String, Integer> reduceByKey = mapToPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});
		reduceByKey.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> t)
					throws Exception {
				return new Tuple2<Integer, String>(t._2, t._1);
			}
		}).sortByKey(false,3).mapToPair(new PairFunction<Tuple2<Integer,String>, String, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Tuple2<Integer, String> t)
					throws Exception {
				return new Tuple2<String,Integer>(t._2,t._1);
			}
		}).foreach(new VoidFunction<Tuple2<String,Integer>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println(t);
			}
		});
	}
}
/**
=================================================================================


19/05/13 17:49:20 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
     (hello,4)
19/05/13 17:49:20 INFO Executor: Finished task 0.0 in stage 4.0 (TID 3). 1165 bytes result sent to driver
19/05/13 17:49:20 INFO TaskSetManager: Starting task 1.0 in stage 4.0 (TID 4, localhost, partition 1,NODE_LOCAL, 1813 bytes)
19/05/13 17:49:20 INFO Executor: Running task 1.0 in stage 4.0 (TID 4)
19/05/13 17:49:20 INFO TaskSetManager: Finished task 0.0 in stage 4.0 (TID 3) in 16 ms on localhost (1/3)
19/05/13 17:49:20 INFO ShuffleBlockFetcherIterator: Getting 1 non-empty blocks out of 1 blocks
19/05/13 17:49:20 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
     (usst,2)
19/05/13 17:49:20 INFO Executor: Finished task 1.0 in stage 4.0 (TID 4). 1165 bytes result sent to driver
19/05/13 17:49:20 INFO TaskSetManager: Starting task 2.0 in stage 4.0 (TID 5, localhost, partition 2,NODE_LOCAL, 1813 bytes)
19/05/13 17:49:20 INFO Executor: Running task 2.0 in stage 4.0 (TID 5)
19/05/13 17:49:20 INFO TaskSetManager: Finished task 1.0 in stage 4.0 (TID 4) in 10 ms on localhost (2/3)
19/05/13 17:49:20 INFO ShuffleBlockFetcherIterator: Getting 1 non-empty blocks out of 1 blocks
19/05/13 17:49:20 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
	(aa,1)
	(haha,1)
	(wordl,1)
	(ab,1)
19/05/13 17:49:20 INFO Executor: Finished task 2.0 in stage 4.0 (TID 5). 1165 bytes result sent to driver
19/05/13 17:49:20 INFO TaskSetManager: Finished task 2.0 in stage 4.0 (TID 5) in 7 ms on localhost (3/3)
=================================================================================
**/

