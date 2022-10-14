package com.amex.training.sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RDDTest1 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf();
		conf.setAppName("rdd-creation-test1");
		//conf.setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("WARN");

		JavaRDD<String> rdd1 = sc.textFile("c:/test/first.txt");
		rdd1.collect().forEach(line -> System.out.println(line));
		sc.close();

	}

}