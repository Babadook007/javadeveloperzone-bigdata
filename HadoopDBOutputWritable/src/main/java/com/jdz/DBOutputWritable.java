package com.jdz;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class DBOutputWritable implements Writable, DBWritable{
	
	private int quantity;
	private String stockCode;
	private String description;
	
	public DBOutputWritable(){}
	public DBOutputWritable(String stockCode,String description, int quantity)
	{
		this.stockCode=stockCode;
		this.description = description;
		this.quantity=quantity;
	}

	public void write(PreparedStatement statement) throws SQLException {
		
		statement.setString(1, stockCode);
		statement.setString(2, description);
		statement.setInt(3, quantity);
	}

	public void readFields(ResultSet resultSet) throws SQLException {
		
		this.stockCode = resultSet.getString(1);
		this.description = resultSet.getString(2);
		this.quantity=resultSet.getInt(3);
	}

	public void write(DataOutput out) throws IOException {
	}

	public void readFields(DataInput in) throws IOException {
	}
}
