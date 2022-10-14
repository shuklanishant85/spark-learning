package com.amex.training.structuredstreams;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

public class StructuredStreamTest1 {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder()
				.appName("structured-stream-test")
				.master("local[*]")
				.getOrCreate();
		spark.sparkContext().setLogLevel("WARN");
		
		String inputDirectory = "c:/structuredcsvdata/test";
		Dataset<Row> df=spark.readStream().schema(StructuredStreamingUtility.employeeSchema())
                .csv(inputDirectory).select("emp_id","emp_name");
        
        try {
            StreamingQuery query=df.writeStream().trigger(Trigger.ProcessingTime(1,TimeUnit.MINUTES))
                    .format("console").start();
            System.out.println("streaming started");
            Thread.sleep(10*60*1000);
            query.stop();
        } catch (TimeoutException | InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
		
	}
	

}
