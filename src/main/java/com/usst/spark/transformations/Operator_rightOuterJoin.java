package com.usst.spark.transformations;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import com.google.common.base.Optional;

import scala.Tuple2;

public class Operator_rightOuterJoin {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("join");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaPairRDD<Integer, String> nameRDD = sc.parallelizePairs(Arrays.asList(
				new Tuple2<Integer, String>(0, "aa"),
				new Tuple2<Integer, String>(1, "a"),
				new Tuple2<Integer, String>(2, "b"),
				new Tuple2<Integer, String>(3, "c")
			),3);
		JavaPairRDD<Integer, Integer> scoreRDD = sc.parallelizePairs(Arrays.asList(
				new Tuple2<Integer, Integer>(1, 100),
				new Tuple2<Integer, Integer>(2, 200),
				new Tuple2<Integer, Integer>(3, 300),
				new Tuple2<Integer, Integer>(4, 400)
		),9);
		
		JavaPairRDD<Integer, Tuple2<Optional<String>, Integer>> rightOuterJoin = nameRDD.rightOuterJoin(scoreRDD);
		System.out.println("leftOuterJoin.partitions().size()--------"+rightOuterJoin.partitions().size());
		rightOuterJoin.foreach(new VoidFunction<Tuple2<Integer,Tuple2<Optional<String>,Integer>>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<Integer, Tuple2<Optional<String>, Integer>> t) throws Exception {
				System.out.println(t);
				
			}
		});
		
       sc.close();

	}
}

/**
=================================================================================
			leftOuterJoin.partitions().size()--------9
			
			(1,(Optional.of(a),100))
			(2,(Optional.of(b),200))
			(3,(Optional.of(c),300))
			(4,(Optional.absent(),400))
=================================================================================
**/
