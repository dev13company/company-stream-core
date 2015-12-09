package com.company.stream.utils;

import java.util.Arrays;

import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;

import scala.Tuple2;

public class SampleJavaStreamJob {
	 

	public static void sampleStreamJob() {
//		StreamingExamples.setStreamingLogLevels();
		JavaStreamingContext jssc = null;
		try{
			jssc = new JavaStreamingContext(new SparkConf()
			.setAppName("NetworkWordCount").setMaster("local[2]"), Durations.seconds(1));
			
			JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
			
			// Split each line into words
			JavaDStream<String> words = lines.flatMap(
			  new FlatMapFunction<String, String>() {
			    public Iterable<String> call(String x) {
			      return Arrays.asList(x.split(" "));
			    }
			  });
			
			// Count each word in each batch
			JavaPairDStream<String, Integer> pairs = words.mapToPair(
			  new PairFunction<String, String, Integer>() {
			    public Tuple2<String, Integer> call(String s) throws Exception {
			      return new Tuple2<String, Integer>(s, 1);
			    }
			  });
			JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
			  new Function2<Integer, Integer, Integer>() {
			    public Integer call(Integer i1, Integer i2) throws Exception {
			      return i1 + i2;
			    }
			  });

			// Print the first ten elements of each RDD generated in this DStream to the console
			wordCounts.print();
			jssc.start();              // Start the computation
			jssc.awaitTermination();   // Wait for the computation to terminate
		} catch(Exception e){
			e.printStackTrace();
		} finally{
			if(null != jssc)
				jssc.stop(false);
		}
	}
	
	public static void main(String[] args) {
		sampleStreamJob();
	}
	
}
