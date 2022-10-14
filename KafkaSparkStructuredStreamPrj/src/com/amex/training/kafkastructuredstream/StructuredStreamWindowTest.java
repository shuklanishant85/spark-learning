package com.amex.training.kafkastructuredstream;

//import static java.lang.Math.*;
import static org.apache.spark.sql.functions.column;
import static org.apache.spark.sql.functions.input_file_name;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.window;

import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;

public class StructuredStreamWindowTest {
    
    public static Column getFileName()
    {
        return input_file_name();
    }

    public static void main(String[] args) {

    	//printTumblingWindow();
        printSlidingWindow();
             
    }

	private static void printSlidingWindow() {
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
        Dataset<Row> resultDF=df.select("Name","Date","Open","High","Low")
                    .groupBy(window(column("Date"), "10 days", "5 days"),column("Name"))
                    .agg(max("High").as("Max"))
                    .orderBy(column("window.start"));
        try {
            StreamingQuery query=resultDF.writeStream()
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
        }
	}

	private static void printTumblingWindow() {
		// TODO Auto-generated method stub
    //  System.out.println(sqrt(25));
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
        Dataset<Row> resultDF=df.select("Name","Date","Open","High","Low")
                    .groupBy(window(column("Date"), "10 days"),column("Name"))
                    .agg(max("High").as("Max"))
                    .orderBy(column("window.start"));
        try {
            StreamingQuery query=resultDF.writeStream()
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
        }
	}

}