package com.amex.training.kafkastructuredstream;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.split;

import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;


public class StructuredStreamWIthKafkaTest {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder()
				.appName("structured-stream-test")
				.master("local[*]")
				.getOrCreate();
		spark.sparkContext().setLogLevel("WARN");
		
		Dataset<Row> df = spark.readStream()
				.format("kafka")
				.option("kafka.bootstrap.servers", "localhost:9092")
				.option("subscribe", "second-topic")
				.load()
				.select(col("value").cast("string"));
		

        Dataset<Row> wordCount=
                df.select(explode(split(col("value")," "))).alias("words")
                .groupBy("words").count();
        try {
            StreamingQuery query= wordCount.writeStream()
            .outputMode(OutputMode.Update())
            .format("console")
            .start();
            
            
            System.out.println("streaming started");
            Thread.sleep(10*60*1000);
            query.stop();
            
            
        } catch (TimeoutException | InterruptedException e) {
            e.printStackTrace();
        }
		
	}
}
