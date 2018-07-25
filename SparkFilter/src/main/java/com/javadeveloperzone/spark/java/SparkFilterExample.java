package com.javadeveloperzone.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;

public class SparkFilterExample {

	/*
	 
	 Function<T, R> R call(T)
	 Take in one input and return one output, for use with things
	 like map and filter.
	 
	 Function2<T1, T2, R> R call(T1, T2)
	 Take in two inputs and return one output, for use with things
	 like aggregate or fold.
	
	 FlatMapFunction<T, R> Iterable<R> call(T) Take in one input and return zero or more outputs, 
	 for use with things like flatMap.
	 */
	
	static class ContainsFunction implements Function<String,Boolean>{

		
		private static final long serialVersionUID = 1L;
		
		private String query;
		
		public ContainsFunction(String query){
			this.query = query;
		}
		
		@Override
		public Boolean call(String input) throws Exception {
			return input.contains(query);
		}
		
	}
	
	public static void main(String[] args) {
		
		SparkConf sparkConf = new SparkConf().setAppName("Apache Spark Java example - Spark Filter");
		
		/*Setting Master for running it from IDE.*/
		sparkConf.setMaster("local[2]");

		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		
		/*Passing Text file as first argument*/
		JavaRDD<String> inputRdd = sparkContext.textFile(args[0]);
		
		/*Creating Info and Error RDD*/
		JavaRDD<String> infoRdd = inputRdd.filter(new ContainsFunction("INFO"));
		
		JavaRDD<String> errorRdd = inputRdd.filter(new ContainsFunction("ERROR"));
		
		/*Java 8 lambda expression to create an inline filter function*/ 
		JavaRDD<String> warningRdd = inputRdd.filter(s -> s.contains("WARNING"));
		
		/*Union the Error and Info RDD content*/
		JavaRDD<String> infoWarningRdd = infoRdd.union(warningRdd);
		
		/*Saving the info,warning RDD to location specified as argument 1*/
		infoWarningRdd.saveAsTextFile(args[1]);
		
		/*Saving the info,warning RDD to location specified as argument 2*/
		errorRdd.saveAsTextFile(args[2]);
		
		/*Stopping Spark Context*/
		sparkContext.stop();
		
		sparkContext.close();
		
	}
}
