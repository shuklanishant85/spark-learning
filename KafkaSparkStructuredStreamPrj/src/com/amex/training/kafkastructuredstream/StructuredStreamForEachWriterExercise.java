package com.amex.training.kafkastructuredstream;

//import static java.lang.Math.*;
import static org.apache.spark.sql.functions.input_file_name;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

public class StructuredStreamForEachWriterExercise {
    
    public static Column getFileName()
    {
        return input_file_name();
    }
    
    public static void main(String[] args) {
        // TODO Auto-generated method stub
    //  System.out.println(sqrt(25));
        SparkSession spark=SparkSession.builder().appName("structured-stream-test-user-data")
                .master("local[*]").getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        String inputDirectory="C:/streaminputdata/test";
        Dataset<Row> df=spark.readStream().schema(StructuredStreamingUtility.userSchema())
                //.option("maxFilesPerTrigger", 2)
                .format("csv")
                .option("path", inputDirectory)
                .load();
        
        
		Dataset<Row> resultDF = df.select("name", "pcode", "age");
        UserWriter writer=new UserWriter();
        try {
            StreamingQuery query=resultDF.writeStream()
                    .outputMode(OutputMode.Append())
                    .trigger(Trigger.ProcessingTime(30, TimeUnit.SECONDS))
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
