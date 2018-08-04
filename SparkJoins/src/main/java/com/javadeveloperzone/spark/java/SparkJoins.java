package com.javadeveloperzone.spark.java;

import java.io.FileNotFoundException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SparkJoins {

    
    public static void main(String[] args) throws FileNotFoundException {
    	
    	SparkConf sparkConf = new SparkConf().setAppName("Apache Spark example - Spark Joins");
        
    	/*Setting Master for running it from IDE.
    	 *User may set more than 1 if user is running it on multicore processor */
		sparkConf.setMaster("local[1]");
    	
    	JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
    	
        JavaRDD<String> userInputRDD = sparkContext.textFile(args[0]);
        
        /*Method 1 :: We are creating PairFunction first, in which each Tuple2 will contain the Key, Value like,
         *KEY :: UserID  
         *VALUE :: <FirstName,LastName> */
        PairFunction<String, String, String> userKeyValueData = new PairFunction<String, String, String>() {
        	
        	public Tuple2<String, String> call(String s) {
                String[] userVaues = s.split(",");

                return new Tuple2<String, String>(userVaues[0], userVaues[1]+","+userVaues[2]);
            }
		};
	
		/*Once the userKeyValue data is ready, we are mapping it using mapToPair function with distinct values
		 *which returns JavaPairRDD.*/
		JavaPairRDD<String,String> userPairs = userInputRDD.mapToPair(userKeyValueData).distinct(); 
        
        JavaRDD<String> addressInputRDD = sparkContext.textFile(args[1]);
       
        /*Method 2 :: We are directly creating JavaPairRDD using mapToPair function and we are passing the new PairFunction
         *with its definition which is returning a Tuple2 object which contains,
         *KEY :: AddressID  
         *VALUE :: <City,State,Country> */
        
        JavaPairRDD<String, String> contactDetailPairs = addressInputRDD.mapToPair(new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String s) {
                String[] contactDetailValues = s.split(",");
                return new Tuple2<String, String>(contactDetailValues[0], contactDetailValues[1]+","+contactDetailValues[2]+","+contactDetailValues[3]);
            }
        });

        /*Default Join operation (Inner join)*/
        JavaPairRDD<String, Tuple2<String, String>> joinsOutput = userPairs.join(contactDetailPairs);
        
        /*Storing the result of inner Join values*/
        joinsOutput.saveAsTextFile(args[2]+"/InnerJoin");

        /*Left Outer join operation*/
        JavaPairRDD<String, Iterable<Tuple2<String, Optional<String>>>> leftJoinOutput = userPairs.leftOuterJoin(contactDetailPairs).groupByKey().sortByKey();
        
        /*Storing values of Left Outer join*/
        leftJoinOutput.saveAsTextFile(args[2]+"/LeftOuterJoin");

        /*Right Outer join operation*/
        JavaPairRDD<String, Iterable<Tuple2<Optional<String>, String>>> rightJoinOutput = userPairs.rightOuterJoin(contactDetailPairs).groupByKey().sortByKey();
        
        /*Storing values of Right Outer join*/
        rightJoinOutput.saveAsTextFile(args[2]+"/RightOuterJoin");

        sparkContext.stop();
        
        sparkContext.close();
        
       /* JavaPairRDD<String, Tuple2<String, Optional<String>>> rddWithJoin = userPairs.leftOuterJoin(transactionPairs);
    
        // mapping of join result
        JavaPairRDD<String, String> mappedRDD = rddWithJoin
                    .mapToPair(tuple -> {
                        if (tuple._2()._2().isPresent()) {
                            //do your operation and return
                        	
                            return new Tuple2<String, String>(tuple._1(), tuple._2()._1());
                        } else {
                            return new Tuple2<String, String>(tuple._1(), "not present");
                        }
                    });*/
        
     /*   JavaPairRDD<String, String> mappedRDD = rddWithJoin
                .mapToPair(new PairFunction<Tuple2<String,Tuple2<String,Optional<String>>>, String, String>() {

					@Override
					public Tuple2<String, String> call(Tuple2<String, Tuple2<String, Optional<String>>> input)
							throws Exception {
						
						
						input.
						
						
						return null;
					}
				});*/
    
    }
}

