package com.amex.training.kafkastreams;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

public class KafkaSparkStreamHDFSTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String topic = "test-topic";
		String kafkaUrl = "localhost:9092";
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "group-1");

		Collection<String> topics = Collections.singletonList(topic);

		SparkConf conf = new SparkConf();
		conf.setMaster("local[*]");
		conf.setAppName("etl-kafka-spark-streaming-app");
		JavaStreamingContext context = new JavaStreamingContext(conf, Durations.seconds(30));
		context.sparkContext().setLogLevel("WARN");
		JavaInputDStream<ConsumerRecord<String, String>> dStream = KafkaUtils.createDirectStream(context,
				LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topics, props));

		dStream.mapToPair(record -> new Tuple2<>(record.key(), record.value())).map(t -> t._2)
				.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
				.mapToPair(word -> new Tuple2<>(word, 1))
				.reduceByKey((a, b) -> a + b)
				.dstream()
				.saveAsTextFiles("hdfs://localhost:9820/training/wc/", "-kafkawc");
		context.start();
		System.out.println("streaming started");
		try {
			Thread.sleep(10 * 60 * 1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		context.stop();

	}

}
