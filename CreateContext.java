package com.github.kpathak;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class CreateContext {
	
	public static void main(String args[])
	{
		
		//Ways to create java spark context.
		
		SparkSession spark = SparkSession
			      .builder()
			      .appName("MyApp")
			      .master("local")
			      .getOrCreate();

		//Way one	    
		JavaSparkContext jsc1 = new JavaSparkContext(spark.sparkContext());
		
		//Way two
		SparkContext sc = spark.sparkContext();
		JavaSparkContext jsc2 = JavaSparkContext.fromSparkContext(sc);
		spark.stop();
	}

}
