package com.amex.training.kafkastructuredstream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;



import static org.apache.spark.sql.functions.*;

import java.util.concurrent.TimeoutException;
public class CSVWithWindowTest2 {

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        SparkSession spark=SparkSession.builder().appName("kafka-structured-stream-window-test")
                .master("local[*]").getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        
        
                String inputDirectory="c:/structuredcsvdata/test";
        Dataset<Row> df=spark.readStream().schema(StructuredStreamingUtility.dataSchema())
                .csv(inputDirectory).select("time_stamp","data");
                
        

        Dataset<Row> eventDF=
                df.select(split(col("time_stamp")," ").as("datetime"),col("data"))
                .withColumn("event_timestamp",element_at(col("datetime"),2)
                        .cast("timestamp"))
                .withColumn("val",col("data").cast("int"))
                .drop("datetime")
                .drop("data");
                
        
        Dataset<Row> resultDF=eventDF
                .withWatermark("event_timestamp", "6 minute")
        		.groupBy(window(col("event_timestamp"),"5 minute"))
                .agg(count("val").as("count"));
        try {
            StreamingQuery query= resultDF.writeStream()
            .format("console")
            .outputMode(OutputMode.Update())
            .option("truncate","false")
            .start();
            
            Thread.sleep(10*60*1000);
            query.stop();
            System.out.println("streaming started");
            
            
            
        } catch (TimeoutException | InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}