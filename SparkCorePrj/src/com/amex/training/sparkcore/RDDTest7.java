package com.amex.training.sparkcore;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class RDDTest7 {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("rdd-word-count-test");
		conf.setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("WARN");

		JavaPairRDD<String, Integer> rdd2 = sc
				.textFile("c:/test/word-count.txt")
				.flatMap(x -> Arrays.asList(x.split(" ")).iterator())	
				.mapToPair(line -> new Tuple2<String, Integer>(line, 1))
				.reduceByKey((x, y) -> x + y);

		rdd2.collect().forEach(t -> System.out.println(t._1 + ": " + t._2));
		sc.close();
	}

}