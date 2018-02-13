package com.javadeveloperzone.spark;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCount {
	
	static class SplitFunction implements FlatMapFunction<String, String>
	{

		private static final long serialVersionUID = 1L;

		@Override
		public Iterator<String> call(String s) {
			return Arrays.asList(s.split(" ")).iterator();
		}
		
	}
	
	public static void main(String[] args)
	{

		SparkConf sparkConf = new SparkConf().setAppName("Simple Application");
		
		//Setting Master for running it from IDE.
		sparkConf.setMaster("local[2]");

		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		JavaRDD<String> textFile = sc.textFile(args[0]);
		
		JavaRDD<String> words = textFile.flatMap(new SplitFunction());
		
		//
		JavaPairRDD<String, Integer> pairs = words
				.mapToPair(new PairFunction<String, String, Integer>() {
					public Tuple2<String, Integer> call(String s) {
						return new Tuple2<String, Integer>(s, 1);
					}
				});
		
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer a, Integer b) {
						return a + b;
					}
				});

		counts.saveAsTextFile(args[1]);
		sc.stop();
		sc.close();
	}
}
