package com.amex.training.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DataSetJDBCTest {

    public static void main(String[] args) {
        SparkSession spark=SparkSession.builder().appName("dataframe-jdbc-test")
                .master("local[*]").getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        //loadDataFromTable(spark);
        
        Dataset<Row> df=spark.read().format("json").option("header",true)
                .load("c:/testjdbcjsondev");
        df.show();
        
        
		//loadIntoDBFromCSV(spark);
		loadIntoDBFromJOSN(spark);

    }

	public static void loadIntoDBFromJOSN(SparkSession spark) {
		Dataset<Row> ds = spark
				.read()
				.format("json")
				.option("header", true)
				.load("c:/test/employee.json");
		
		ds.where("designation='Developer'")
				.write()
				.format("jdbc")
				.option("url", "jdbc:mysql://localhost:3306/trainingdb")
				.option("dbtable", "developers")
				.option("user", "root").option("password", "rps@12345")
				.save();

//		spark.sql("use default");
//		ds.where("designation='Developer'")
//				.write()
//				.format("jdbc")
//				.option("url", "jdbc:mysql://localhost:3306/trainingdb")
//				.option("dbtable", "developers")
//				.option("user", "root")
//				.option("password", "rps@12345")
//				.insertInto("trainingdb.developers");
	}

	public static void loadIntoDBFromCSV(SparkSession spark) {
		Dataset<Row> ds = spark
				.read()
				.format("csv")
				.option("header", true)
				.load("c:/test/employee.csv");

		ds.where("designation='Developer'")
				.write()
				.format("jdbc")
				.option("url", "jdbc:mysql://localhost:3306/trainingdb")
				.option("dbtable", "developers")
				.option("user", "root").option("password", "rps@12345")
				.save();
	}

	public static void loadDataFromTable(SparkSession spark) {
		Dataset<Row> empDS=spark
        		.read()
        		.format("jdbc")
        		.option("url","jdbc:mysql://localhost:3306/trainingdb")
        		.option("dbtable","employee")
        		.option("user","root")
        		.option("password","rps@12345")
                .load();
        
        empDS.where("designation='developer'").show();
        empDS.where("designation='developer'").write().json("c:/testjdbcjsondev");
	}
    
    public static StructType employeeSchema() {
    	return new StructType(
    			new StructField[] {
    	    			new StructField("emp_id", DataTypes.IntegerType, true, Metadata.empty()),
    	    			new StructField("emp_name", DataTypes.StringType, true, Metadata.empty()),
    	    			new StructField("description", DataTypes.StringType, true, Metadata.empty())
    			}
    			);
    }

}
