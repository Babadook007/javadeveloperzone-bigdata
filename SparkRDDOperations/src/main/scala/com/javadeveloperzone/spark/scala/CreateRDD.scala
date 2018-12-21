package com.javadeveloperzone.spark.scala

import org.apache.spark._
import org.apache.spark.SparkContext._

object CreateRDD{
  
   def main(args: Array[String]) 
   {
     
     /*By default we are setting local so it will run locally with one thread 
      *Specify: "local[2]" to run locally with 2 cores, OR 
      *        "spark://master:7077" to run on a Spark standalone cluster */
     
      val sparkContext = new SparkContext("local","Spark RDD Example using Scala",
          System.getenv("SPARK_HOME"))
      
      /*First way to create RDD is, Read data from external data source,
       *here we are reading an external input from File*/
      val input = sparkContext.textFile(args(0))
      
      /*Creating RDD from lines on input file*/
      val words = input.flatMap(line => line.split(" "))
      
      /*Performing mapping and reducing operation*/
      val counts = words.map(word => (word, 1)).reduceByKey{case (x,y) => x + y}
      
      /*Saving the result file to the location that we have specified as args[1]*/
      counts.saveAsTextFile(args(1))
      
      
      /*Second way to create RDD is, Parallelizing a collection in driver program.
       *We can simply call SparkContext's parallelize() method to create RDD. */
      
      val likes = sparkContext.parallelize(List("spark", "I like spark"))
      
    }
}