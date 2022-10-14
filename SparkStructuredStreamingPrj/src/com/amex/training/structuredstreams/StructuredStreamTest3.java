package com.amex.training.structuredstreams;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.input_file_name;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.year;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

public class StructuredStreamTest3 {

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        SparkSession spark=SparkSession.builder().appName("structured-stream-test")
                .master("local[*]").getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        String inputDirectory="C:/streaminputdata/test";
        Dataset<Row> df=spark.readStream().schema(StructuredStreamingUtility.stockSchema())
                .option("maxFilesPerTrigger", 2)
                .format("csv")
                .option("path", inputDirectory)
                .load()
                .withColumn("Name", getFileName());
        Dataset<Row> stockDF=df.groupBy(col("Name"),year(col("Date")).as("Year"))
                .agg(max("High").as("Max"));
        
        try {
            StreamingQuery query=stockDF.writeStream()
                    .format("console")
                    .outputMode(OutputMode.Complete())
                    .option("truncate", false)
                    //.option("numRows", 3)
                    .start();
            System.out.println("streaming started");
            Thread.sleep(10*60*1000);
            query.stop();
        } catch (TimeoutException | InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }}
    
    public static Column getFileName() {
    	return input_file_name();
    }

	public static void streamJsonData() {
		// TODO Auto-generated method stub
        SparkSession spark=SparkSession.builder().appName("structured-stream-test")
                .master("local[*]").getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        String inputDirectory="c:/structuredjsondata/test";
        Dataset<Row> df=spark.readStream().schema(StructuredStreamingUtility.employeeSchema())
                .csv(inputDirectory).where("designation='Developer'");
        
        try {
            StreamingQuery query=df.writeStream().trigger(Trigger.ProcessingTime(1,TimeUnit.MINUTES))
                    .format("json")
                    .option("checkpointLocation", "testcheckpoint")
                    .start("c:/structuredjsondata");
            System.out.println("streaming started");
            Thread.sleep(10*60*1000);
            query.stop();
        } catch (TimeoutException | InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
	}

}








