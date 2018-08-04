package com.javadeveloperzone.spark.scala

import org.apache.spark._
import org.apache.spark.SparkContext._

object SparkJoins {
  
  def main(args: Array[String]) 
   {
     
     /*By default we are setting local so it will run locally with one thread 
      *Specify: "local[2]" to run locally with 2 cores, OR 
      *        "spark://master:7077" to run on a Spark standalone cluster */
     
      val sparkContext = new SparkContext("local","Apache Spark example - Spark Joins",
          System.getenv("SPARK_HOME"))
      
      /*Reading input from User Data File*/
      val userInputRDD = sparkContext.textFile(args(0))
      
      /*We are creating User Pairs which will contain the Key, Value like,
       *KEY :: UserID  
       *VALUE :: <FirstName,LastName> */
      val userPairs = userInputRDD.map(line => (line.split(",")(0), line.split(",")(1)+","+line.split(",")(2)))
      
      /*Reading input from Address Data File*/
      val addressInputRDD = sparkContext.textFile(args(1))
      
      /*We are directly Address Pairs which will contain the Key, Value like,,
       *KEY :: AddressID  
       *VALUE :: <City,State,Country> */
      val addressPairs = addressInputRDD.map(line => (line.split(",")(0), line.split(",")(1)+","+line.split(",")(2)+","+line.split(",")(3)))
      
      /*Default Join operation (Inner join)*/
      val innerJoinResult = userPairs.join(addressPairs);
      
      innerJoinResult.saveAsTextFile(args(2)+"/InnerJoin")
      
      /*Left Outer join operation*/
      val leftOuterJoin = userPairs.leftOuterJoin(addressPairs)
      
      /*Storing values of Left Outer join*/
      leftOuterJoin.saveAsTextFile(args(2)+"/LeftOuterJoin")
      
      /*Right Outer join operation*/
      val rightOuterJoin = userPairs.rightOuterJoin(addressPairs)
      
      /*Storing values of Right Outer join*/
      rightOuterJoin.saveAsTextFile(args(2)+"/RightOuterJoin")
      
      sparkContext.stop()
      
    }
}