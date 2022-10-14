package com.amex.training.sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RDDTest5 {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("rdd-map-filter-test");
		//conf.setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("WARN");

		JavaRDD<String> rdd1 = sc.textFile("c:/test/fourth.txt");
		rdd1.cache().collect().forEach(x -> System.out.println(x));
		System.out.println(rdd1.getNumPartitions());
		rdd1.filter(x -> x.startsWith("I")).map(x -> x.toUpperCase()).collect().forEach(x -> System.out.println(x));
		rdd1.filter(x -> x.startsWith("I")).map(x -> x.toUpperCase()).repartition(1).saveAsTextFile("c:/test/fifth");

		sc.close();
	}

}