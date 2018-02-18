package com.javadeveloperzone.spark.java;

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
	
	/*For Simplicity,
	 *We are creating custom Split function, 
	 *so it makes code easier to understand 
	 *We are implementing FlatMapFunction.*/
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

		SparkConf sparkConf = new SparkConf();
				
		sparkConf.setAppName("Spark WordCount example using Java");
		
		//Setting Master for running it from IDE.
		sparkConf.setMaster("local[2]");

		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		JavaRDD<String> textFile = sc.textFile(args[0]);
		
		JavaRDD<String> words = textFile.flatMap(new SplitFunction());
		
		/*Below code generates Pair of Word with count as one 
		 *similar to Mapper in Hadoop MapReduce*/
		JavaPairRDD<String, Integer> pairs = words
				.mapToPair(new PairFunction<String, String, Integer>() {
					public Tuple2<String, Integer> call(String s) {
						return new Tuple2<String, Integer>(s, 1);
					}
				});
		
		/*Below code aggregates Pairs of Same Words with count
		 *similar to Reducer in Hadoop MapReduce  
		 */
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(
				new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer a, Integer b) {
						return a + b;
					}
				});

		counts.saveAsTextFile(args[1]);
		sc.stop();
		sc.close();
	}
}
