package com.github.kamalpathak;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class Assignment {

	public static void main(String args[]) {

		SparkSession spark = SparkSession.builder().appName("Spark Cleansing APP").master("local").getOrCreate();
		runCleaningAndTransform(spark);
		spark.stop();

	}

	private static void runCleaningAndTransform(SparkSession spark) {

		SparkContext sc = spark.sparkContext();
		JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);

		// Load the input data, which is a text file read from the command line
		JavaRDD<String> input = jsc.textFile("emp.txt");

		JavaRDD<String> cleanInputRDD = input.map(x -> {
			x = x.replaceAll("\"", "");
			StringBuffer strBuffer = new StringBuffer();
			for (String str : x.split(",")) {
				str = str.trim();
				strBuffer.append(str + ",");
			}
			String strFinal = strBuffer.toString();
			x = strFinal.substring(0, strFinal.lastIndexOf(","));
			System.out.println("Strfinal=" + strFinal);
			return x;
		});

		List<String> inputTwoRow = input.take(2);
		List<String> firstTwoRowClean = cleanInputRDD.take(2);
		List<String> finalTwoRow = new ArrayList<>();
		finalTwoRow.add(0, firstTwoRowClean.get(0));
		finalTwoRow.add(1, inputTwoRow.get(1));
		String strSchema = inputTwoRow.get(1);

		String[] schemaTokens = strSchema.split(",");
		final int noOfColumns = schemaTokens.length;
		// String[] headerTokens=

		String headerRow = finalTwoRow.get(0);
		String schemaRow = firstTwoRowClean.get(1);

		String[] headerTokens = headerRow.split(",");

		JavaRDD<String> output = jsc.parallelize(finalTwoRow);
		output.saveAsTextFile("schema");

		JavaRDD<String> inputFilterRDD = cleanInputRDD
				.filter(x -> !(x.equalsIgnoreCase(headerRow) || x.equalsIgnoreCase(schemaRow)));

		inputFilterRDD.saveAsTextFile("staging.cleaned");

		Dataset<Row> schemaDF = spark.read().format("csv").option("inferschema", "true").option("header", "true")
				.load("schema");

		schemaDF.show();
		// print schema
		schemaDF.printSchema();

		StructType schema = schemaDF.schema();
		schema = schema.add("_corrupt_record", DataTypes.StringType, true);

		Dataset<Row> employee = spark.read().format("csv").option("header", "false").option("inferSchema", "false")
				.option("delimiter", ",").option("quote", "\"").schema(schema).load("staging.cleaned");

		Dataset<Row> employeeDS=employee.na().fill(0);
		
		employeeDS.show();
		employeeDS.printSchema();

		Dataset<Row> employeeDSC = employeeDS.withColumn("comment", employeeDS.col("_corrupt_record").isNotNull());
		employeeDSC.show();
		employeeDSC.printSchema();

		// Creates a temporary view using the DataFrame
		employeeDSC.createOrReplaceTempView("employee");

		StringBuffer query = new StringBuffer();
		for (String hToken : headerTokens) {
			query = query.append(hToken + ", ");
		}
		String strQuery = query.toString();
		strQuery = strQuery.substring(0, strQuery.lastIndexOf(","));
		System.out.println("Query==" + strQuery);

		// SQL can be run over a temporary view created using DataFrames
		Dataset<Row> correctRecords = spark
				.sql("SELECT " + strQuery + " FROM employee where comment=false sort by name");
		
		correctRecords.write().format("csv").option("overwrite", "true").option("header", "true").save("output.txt");

		// malformed records
		Dataset<Row> corruptRecords = spark
				.sql("SELECT " + strQuery + ",_corrupt_record FROM employee where comment=true sort by name");


		JavaRDD<Row> corrupt = corruptRecords.toJavaRDD();

		// corrupt records as string.
		JavaRDD<String> corruptStr = corrupt.map(a -> {

			return a.getString(a.fieldIndex("_corrupt_record"));

		});

		// tuple of schema e.g (name, StringType) (age, IntegerType) ..
		scala.Tuple2<String, String>[] tuple = schemaDF.dtypes();

		JavaRDD<String> mapOutputRDD = corruptStr.map(x -> {
			String str[] = x.split(",");
			// System.out.println("number of columns=" + noOfColumns);
			// System.out.println("number of tokens=" + str.length);
			if (str.length != noOfColumns) {
				x = "\"The number of columns in the record doesn't match file header spec.\"" + "," + x;
			} else {
				StringBuffer columnBuffer = new StringBuffer();

				for (int i = 0; i < str.length; i++) {
					String token = str[i];
					if (tuple[i]._2.toString().equalsIgnoreCase("IntegerType")) {
						if (!isInteger(token)) {
							columnBuffer.append(tuple[i]._1 + ";");
						}

					} else if (tuple[i]._2.toString().equalsIgnoreCase("DoubleType")) {
						if (!isDouble(token)) {
							columnBuffer.append(tuple[i]._1 + ";");
						}

					} else if (tuple[i]._2.toString().equalsIgnoreCase("StringType")) {

						if (token.matches(".*\\d.*")) {
							columnBuffer.append(tuple[i]._1 + ";");
						}
					}

				}
				String columns = columnBuffer.toString();
				columns = columns.substring(0, columns.lastIndexOf(";"));
				x = "\"The datatypes of columns:[" + columns
						+ "] doesn't match the datatypes specified in the first test record.\"" + "," + x;
			}
			return x;
		});

		for (String line : mapOutputRDD.collect()) {
			System.out.println(line);
		}

		StringBuffer strBuff = new StringBuffer("error_message,");
		for (int i = 0; i < tuple.length; i++) {
			strBuff.append(tuple[i]._1 + ",");
		}
		String schemaError = strBuff.toString();
		schemaError = schemaError.substring(0, schemaError.lastIndexOf(","));
		List<String> tempList = new ArrayList<>();
		tempList.add(schemaError);
		JavaRDD<String> schemaRDD = jsc.parallelize(tempList);
		JavaRDD<String> sortedRDD=mapOutputRDD.sortBy(x -> x.split(",")[1], true, 1);
		for (String str : sortedRDD.collect()) {
			System.out.println("after sort-" + str);
		}
		JavaRDD<String> joinedRDD = schemaRDD.union(sortedRDD);
		joinedRDD.saveAsTextFile("quarantine.txt");
		
	}

	public static boolean isInteger(String s) {
		try {
			Integer.parseInt(s);
		} catch (NumberFormatException e) {
			return false;
		} catch (NullPointerException e) {
			return false;
		}
		// only got here if we didn't return false
		return true;
	}

	public static boolean isDouble(String s) {
		try {
			// Integer.parseInt(s);
			Double.parseDouble(s);
		} catch (NumberFormatException e) {
			return false;
		} catch (NullPointerException e) {
			return false;
		}
		// only got here if we didn't return false
		return true;
	}

}
