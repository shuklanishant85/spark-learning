package com.amex.training.kafkastructuredstream;

import java.util.concurrent.TimeoutException;
import static org.apache.spark.sql.functions.*;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

public class StructuredStreamWithKafkaSinkExercise {
	
	 public static void main(String[] args) {
	        
	        SparkSession spark=SparkSession.builder().appName("structured-stream-test")
	                .master("local[*]").getOrCreate();
	        spark.sparkContext().setLogLevel("WARN");
	        String inputDirectory="C:/streaminputdata/test";
	        Dataset<Row> df=spark.readStream().schema(StructuredStreamingUtility.employeeSchema())
	                .option("maxFilesPerTrigger", 2)
	                .format("csv")
	                .option("path", inputDirectory)
	                .load();
	                
	        Dataset<Row> resultDF=df.withColumn("value", 
	                concat_ws(":", column("emp_id"),column("emp_name"),column("designation")));
	        
	        try {
	            StreamingQuery query=resultDF.writeStream().
	            format("kafka").option("topic","emp-topic")
	            .option("kafka.bootstrap.servers", "localhost:9092")
	            .option("checkpointLocation", "next-check-point")
	            .start();
	            System.out.println("streaming started");
	            Thread.sleep(10*60*1000);
	            query.stop();
	            
	        } catch (TimeoutException | InterruptedException e) {
	            e.printStackTrace();
	        }
	       
	    }

}
