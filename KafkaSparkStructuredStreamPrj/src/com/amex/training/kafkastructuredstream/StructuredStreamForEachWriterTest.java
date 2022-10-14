package com.amex.training.kafkastructuredstream;

//import static java.lang.Math.*;
import static org.apache.spark.sql.functions.input_file_name;

import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;

public class StructuredStreamForEachWriterTest {
    
    public static Column getFileName()
    {
        return input_file_name();
    }
    
    

    public static void main(String[] args) {
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
        
        
        Dataset<Row> resultDF=df.select("Name","Date","Open");
        StockWriter writer=new StockWriter();
        try {
            StreamingQuery query=resultDF.writeStream()
                    .outputMode(OutputMode.Append())
                    .foreach(writer)
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
