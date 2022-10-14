package com.amex.training.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataSetTest1 {
	
	public static void main(String[] args) {
		SparkSession spark = SparkSession
				.builder()
				.appName("spark-dataset-csv-test")
				.master("local[*]")
				.getOrCreate();
		spark.sparkContext().setLogLevel("WARN");
		
		//Dataset<Row> ds = spark.read().csv("c:/test/employee.csv");
		Dataset<Row> ds = spark
				.read()
				.format("csv")
				.option("header", "true")
				.load("c:/test/employee.csv");
		
		ds.show();
		
	}

}
