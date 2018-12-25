package com.javadeveloperzone.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import javax.print.DocFlavor;
import java.util.List;

public class FunctionExample {

    static  class OddEvenFunction implements Function<String,String>{

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

        JavaRDD<String> oddEvenRDD = stringJavaRDD.map(new OddEvenFunction());

        List<String> oddEvenList = stringJavaRDD.collect();

//        System.out.println("Odd Even List::"+oddEvenList.toString());

        oddEvenRDD.saveAsTextFile(args[1]);

    }
}
