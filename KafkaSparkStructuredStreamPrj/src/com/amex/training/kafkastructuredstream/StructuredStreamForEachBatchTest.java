package com.amex.training.kafkastructuredstream;

//import static java.lang.Math.*;
import static org.apache.spark.sql.functions.input_file_name;
import static org.apache.spark.sql.functions.lit;

import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;

public class StructuredStreamForEachBatchTest {
    
    public static Column getFileName()
    {
        return input_file_name();
    }
    
    public static void saveToDB(Dataset<Row>  df,Object batchId)
    {
        df.withColumn("batchId", lit(batchId))
        .write()
        .format("jdbc")
        .option("url", "jdbc:mysql://localhost:3306/trainingdb")
        .option("dbtable","stock").option("user","root").option("password","rps@12345")
        .mode(SaveMode.Append)
        .save();
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
        df.printSchema();
        
        Dataset<Row> resultDF=df.select("Name","Date","Open");
        
        try {
            StreamingQuery query=resultDF.writeStream()
                    .outputMode(OutputMode.Append())
                    .foreachBatch((df1,id)->saveToDB(df1, id))
                    .start();
            System.out.println("streaming started");
            Thread.sleep(10*60*1000);
            query.stop();
        } catch (TimeoutException | InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        
    }}