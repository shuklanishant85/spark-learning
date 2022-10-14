package com.amex.training.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DataSetTest3 {

    public static void main(String[] args) {
        SparkSession spark=SparkSession.builder().appName("dataframe-test")
                .master("local[*]").getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        Dataset<Row> empDS=spark.read().format("json").option("header",true)
                .schema(employeeSchema())
                .load("c:/test/employee.json");
        Dataset<Row> developers=empDS.where("designation='Developer'").select("id","name");
        developers.write().save("c:/developersJava");
        empDS.where("designation='Accountant'").select("id","name").write().format("csv")
                .option("header", true).save("c:/accountantJava");
        
        empDS.where("designation='Architect'").select("id","name").write().format("json").
        save("c:/architectsJava");


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
