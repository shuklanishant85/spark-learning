package com.amex.training.structuredstreams;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

public class StructuredStreamTest2 {

	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder()
				.appName("structured-json-app")
				.master("local[*]")
				.getOrCreate();

		spark.sparkContext().setLogLevel("WARN");
		
		String inputDirectory = "c:/structuredjsondata/test";
		
		Dataset<Row> ds = spark.readStream()
				.schema(StructuredStreamingUtility.accountSchema())
				.json(inputDirectory).select("acc_no");
		
		try {
			StreamingQuery query = ds.writeStream().trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES)).format("console").start();
			System.out.println("streaming started");
			Thread.sleep(10*60*1000);
			query.stop();
		} catch (TimeoutException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		
	}

}
