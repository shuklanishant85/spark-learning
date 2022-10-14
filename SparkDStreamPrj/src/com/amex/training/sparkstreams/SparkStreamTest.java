package com.amex.training.sparkstreams;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class SparkStreamTest {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local[*]");
		conf.setAppName("simple-stream-app");
		JavaStreamingContext context =  new JavaStreamingContext(conf, Durations.seconds(30));
		context.sparkContext().setLogLevel("WARN");
		
		//looks for new files in the specific directory and processes them
		JavaDStream<String> dStream = context.textFileStream("c:/textstream/test");
        dStream
        	.flatMap(line->Arrays.asList(line.split(" ")).iterator())
        	.mapToPair(word->new Tuple2<>(word, 1))
        	.reduceByKey((a,b)->a+b).print();
        

        context.start();
        System.out.println("streaming started");
        try {
			Thread.sleep(10*60*1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
        context.stop();
        
        context.close();
		
	}

}
