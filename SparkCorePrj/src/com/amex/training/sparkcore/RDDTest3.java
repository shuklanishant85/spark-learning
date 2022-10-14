package com.amex.training.sparkcore;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RDDTest3 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf();
		conf.setAppName("rdd-map-test");
		conf.setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("WARN");

		JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("Watermelon","Banana","Papaya"));
		rdd1.map(x -> x.length()).map(x -> x * 2).collect().forEach(line -> System.out.println(line));
		sc.close();
	}

}