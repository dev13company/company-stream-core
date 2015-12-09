package com.company.stream.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.collect.Lists;

import scala.Tuple2;

public class KafkaStreamJob {

	private static final Pattern SPACE = Pattern.compile(" ");
	
	public static void getKafkaStreamCount(){
		JavaStreamingContext jssc = null;
		try{
			jssc = new JavaStreamingContext(new SparkConf().setMaster("local[2]").
					setAppName(" kafka word count"), new Duration(2000));
			
			int numThreads = Integer.parseInt("1");
		    Map<String, Integer> topicMap = new HashMap<String, Integer>();
		    String[] topics = "spark-stream".split(",");
		    for (String topic: topics) {
		      topicMap.put(topic, numThreads);
		    }

		    JavaPairReceiverInputDStream<String, String> messages =
		            KafkaUtils.createStream(jssc, "localhost:2181", "spark-stream", topicMap);
		    
		    JavaDStream<String> lines = messages.map(
		    		new Function<Tuple2<String,String>, String>() {

						public String call(Tuple2<String, String> tuple2)
								throws Exception {
							return tuple2._2();
						}
					});

		      JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
		        public Iterable<String> call(String x) {
		          return Lists.newArrayList(SPACE.split(x));
		        }
		      });

		      JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
		        new PairFunction<String, String, Integer>() {
		          public Tuple2<String, Integer> call(String s) {
		            return new Tuple2<String, Integer>(s, 1);
		          }
		        }).reduceByKey(
		        		new Function2<Integer, Integer, Integer>() {
		          public Integer call(Integer i1, Integer i2) {
		            return i1 + i2;
		          }
		        });

		      wordCounts.print();
		      jssc.start();
		      jssc.awaitTermination();
		} catch(Exception e){
			e.printStackTrace();
		} finally{
			if(null != jssc)
				jssc.stop(false);
		}
	}
	
	public static void main(String[] args) {
		getKafkaStreamCount();
	}
}
