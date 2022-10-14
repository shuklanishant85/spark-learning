package com.amex.training.kafkastructuredstream;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.element_at;
import static org.apache.spark.sql.functions.split;

import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
public class CSVWithWindowTest {

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
                .withColumn("event_timestamp",element_at(col("datetime"),1))
                .drop("datetime");
                
        
        
        try {
            StreamingQuery query= eventDF.writeStream()
            .format("console")
            .start();
            
            Thread.sleep(10*60*1000);
            query.stop();
            System.out.println("streaming started");
            Thread.sleep(10*60*1000);
            query.stop();
            
            
		} catch (TimeoutException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
