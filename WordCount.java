package com.github.kpathak;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class WordCount {

	public static void main(String args[]) {
		
		Scanner scan=new Scanner(System.in);
		System.out.println("Enter file name");
		String fileName=scan.nextLine();
		
		

		SparkSession spark = SparkSession.builder().appName("WordCount").master("local").getOrCreate();

		JavaRDD<String> lines = spark.read().textFile(fileName).javaRDD();
		
		for(String line:lines.collect())
		{
			//Print each lines
			System.out.println(line);
		}
		
		System.out.println(lines.toString());
		
		JavaRDD<String> words=lines.flatMap(x-> Arrays.asList(x.split(" ")).iterator());
		
		for(String str:words.collect())
		{
			//print each word
			System.out.println(str);
			
		}
		
		//Returns (word,1) e.g (abc,1) or (It,1)
		JavaPairRDD<String, Integer> ones= words.mapToPair(x->new Tuple2<>(x,1));
		
		//Sum of all ones for each word e.g (It, 3)
		JavaPairRDD<String, Integer> counts=ones.reduceByKey((i1,i2)->i1+i2);
		//words.mapToPair
		
		List<Tuple2<String, Integer>> output=counts.collect();
		
		for(Tuple2<String, Integer> t:output)
		{
			//Print each tuple
			System.out.println(t._1 +":"+t._2);
		}
		
		spark.stop();
	}

}
