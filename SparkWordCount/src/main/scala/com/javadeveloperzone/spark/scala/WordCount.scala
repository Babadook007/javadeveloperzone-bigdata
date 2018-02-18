package com.javadeveloperzone.spark.scala

import org.apache.spark._
import org.apache.spark.SparkContext._

object WordCount {
  
   def main(args: Array[String]) 
   {
     
     /*By default we are setting local so it will run locally with one thread 
      *Specify: "local[4]" to run locally with 4 cores, OR 
      *        "spark://master:7077" to run on a Spark standalone cluster */
     
      val sparkContext = new SparkContext("local","Spark WordCount example using Scala",
          System.getenv("SPARK_HOME"))
      
      /*Reading input from File*/
      val input = sparkContext.textFile(args(0))
      
      /*Creating flat map from lines on input file*/
      val words = input.flatMap(line => line.split(" "))
      
      val counts = words.map(word => (word, 1)).reduceByKey{case (x,y) => x + y}
          counts.saveAsTextFile(args(1))
      
    }
}