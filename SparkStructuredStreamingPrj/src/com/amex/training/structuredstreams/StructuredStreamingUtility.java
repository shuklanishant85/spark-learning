package com.amex.training.structuredstreams;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class StructuredStreamingUtility {

	public static StructType employeeSchema() {
		return new StructType(
				new StructField[] { new StructField("emp_id", DataTypes.IntegerType, true, Metadata.empty()),
						new StructField("emp_name", DataTypes.StringType, true, Metadata.empty()),
						new StructField("designation", DataTypes.StringType, true, Metadata.empty()) });
	}

	public static StructType accountSchema() {
		return new StructType(
				new StructField[] { new StructField("acc_no", DataTypes.IntegerType, false, Metadata.empty()),
						new StructField("acc_type", DataTypes.StringType, true, Metadata.empty()),
						new StructField("balance", DataTypes.DoubleType, false, Metadata.empty())

				});
	}
	
	public static StructType stockSchema() {
		return new StructType(
				new StructField[] { new StructField("Date", DataTypes.StringType, true, Metadata.empty()),
						new StructField("Open", DataTypes.DoubleType, true, Metadata.empty()),
						new StructField("High", DataTypes.DoubleType, true, Metadata.empty()),
						new StructField("Low", DataTypes.DoubleType, true, Metadata.empty()),
						new StructField("Close", DataTypes.DoubleType, true, Metadata.empty()),
						new StructField("Volumne", DataTypes.LongType, true, Metadata.empty())});
	}

}
