package com.javadeveloperzone.spark.java;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class FunctionExample {

    static class OddEvenFunction implements Function<String,String>{

        @Override
        public String call(String input){
            return ((Integer.parseInt(input) % 2) == 0 ? "Even" : "Odd");
        }
    }

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("Function Example using Java");

        sparkConf.setMaster("local[1]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> stringJavaRDD =  sparkContext.textFile(args[0]);

        /*Java Function Passing with named class.*/
        JavaRDD<String> oddEvenRDD = stringJavaRDD.map(new OddEvenFunction());
        
        JavaPairRDD<String,Integer> pairedRDD= oddEvenRDD.mapToPair(new PairFunction<String, String, Integer>() {
        	
        	@Override
        	public Tuple2<String, Integer> call(String input) throws Exception {
        		
        		Tuple2<String,Integer> result = new Tuple2<String, Integer>(input,1);
        		
        		return result;
        	}
		});
        
        JavaPairRDD<String,Integer> pairedRDDCounts = pairedRDD.reduceByKey(new Function2<Integer,Integer,Integer>(){
        	
        	@Override
        	public Integer call(Integer v1, Integer v2) throws Exception {
        		
        		return v1+v2;
        	}
        	
        });
        
        List<String> oddEvenList = stringJavaRDD.collect();

//        System.out.println("Odd Even List::"+oddEvenList.toString());

//        oddEvenRDD.saveAsTextFile(args[1]);
        pairedRDDCounts.saveAsTextFile(args[1]);
        sparkContext.stop();
        sparkContext.close();
        

    }
}
