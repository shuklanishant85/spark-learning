package com.amex.training.sparkcore;

import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RDDTest4 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf();
		conf.setAppName("rdd-flatmap-test");
		conf.setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("WARN");

		JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("Watermelon is Red","Banana is green","Papaya is Yellow"));
		System.out.println(rdd1.getNumPartitions());
		rdd1.flatMap(x -> Arrays.asList(StringUtils.split(x, " "))
				.iterator()).repartition(1).saveAsTextFile("c:/testFlatMap2");
		sc.close();
	}

}