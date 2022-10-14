package com.amex.training.kafkastructuredstream;

import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;

public class UserWriter extends ForeachWriter<Row> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void close(Throwable errorOrNull) {
		// TODO Auto-generated method stub
		System.out.println("closing");
	}

	@Override
	public boolean open(long partitionId, long epochId) {
		// TODO Auto-generated method stub
		System.out.println("partition id: " + partitionId + "\tepochId: " + epochId);
		return true;
	}

	@Override
	public void process(Row value) {
		// TODO Auto-generated method stub
		System.out.println("BATCH TIME: " + System.currentTimeMillis());
		System.out.println("--------------------------------");
		System.out.println("Name: " + value.getAs("name").toString());
		System.out.println("Pcode:" + value.getAs("pcode").toString());
		System.out.println("Age:" + value.getAs("age").toString());

	}

}