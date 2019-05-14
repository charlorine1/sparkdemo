package com.usst.spark.transformations;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * join:两个rdd  执行join，partition是最大的那个，比如下面的partition=9  max（3，9）
 * 
 * 
 * 
 * */
public class Operator_join {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("join");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaPairRDD<Integer, String> nameRDD = sc.parallelizePairs(Arrays.asList(
					new Tuple2<Integer, String>(0, "aa"),
					new Tuple2<Integer, String>(1, "a"),
					new Tuple2<Integer, String>(1, "b"),
					new Tuple2<Integer, String>(2, "dd"),
					new Tuple2<Integer, String>(3, "c")
				),3);
		JavaPairRDD<Integer, Integer> scoreRDD = sc.parallelizePairs(Arrays.asList(
				new Tuple2<Integer, Integer>(1, 100),
				new Tuple2<Integer, Integer>(2, 200),
				new Tuple2<Integer, Integer>(3, 300),
				new Tuple2<Integer, Integer>(4, 400)
		),4);
		
		JavaPairRDD<Integer, Tuple2<String, Integer>> join = nameRDD.join(scoreRDD);
		System.out.println("join.partitions().size()--------"+join.partitions().size());
		join.foreach(new VoidFunction<Tuple2<Integer,Tuple2<String,Integer>>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<Integer, Tuple2<String, Integer>> t)
					throws Exception {
				System.out.println(t);
			}
		});
			
        sc.close();
		
	}

}


/**
 * 分区等于较大的分区，合并后小的分区就是好比放到了大的分区里面
=================================================================================
			leftOuterJoin.partitions().size()--------9
			
			(1,(a,100))
			(1,(b,100))
			(2,(b,200))
			(3,(c,300))
=================================================================================
**/
