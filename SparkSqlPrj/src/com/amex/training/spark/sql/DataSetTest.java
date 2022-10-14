package com.amex.training.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataSetTest {
	
	public static void main(String[] args) {
		SparkSession spark = SparkSession
				.builder()
				.appName("spark-dataset-test")
				.master("local[*]")
				.getOrCreate();
		spark.sparkContext().setLogLevel("WARN");
		
		Dataset<Row> ds = spark.read().json("c:/test/users.json");
		ds.show();
		
	}

}
