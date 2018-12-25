package com.javadeveloperzone.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;
import java.util.List;

public class FlatMapExample {

    /*For Simplicity,
     *We are creating custom Split function,so it makes code easier to understand
     *We are implementing FlatMapFunction interface.*/
    static class SplitFunction implements FlatMapFunction<String, String>
    {

        private static final long serialVersionUID = 1L;

        @Override
        public Iterable<String> call(String input) {
            return Arrays.asList(input.split(" "));
        }

    }


    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();

        sparkConf.setAppName("FlatMap example using Java");

        sparkConf.setMaster("local[1]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> textFile = sparkContext.textFile(args[0]);

        /*Creating RDD of words from each line of input file*/
        JavaRDD<String> words = textFile.flatMap(new SplitFunction());

//        List<String> wordsList = words.collect();

//        System.out.println("wordsList ::"+wordsList.toString());

        words.saveAsTextFile(args[1]);

        sparkContext.stop();

        sparkContext.close();

    }

}
