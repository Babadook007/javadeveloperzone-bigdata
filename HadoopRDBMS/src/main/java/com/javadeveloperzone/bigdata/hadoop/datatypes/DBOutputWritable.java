package com.javadeveloperzone.bigdata.hadoop.datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class DBOutputWritable implements Writable, DBWritable{
	
	int count;
	String userName;
	
	public DBOutputWritable(){}
	public DBOutputWritable(String userName, int count)
	{
		this.userName=userName;
		this.count = count;
	}

	public void write(PreparedStatement statement) throws SQLException {
		statement.setString(1, userName);
		statement.setInt(2, count);
	}

	public void readFields(ResultSet resultSet) throws SQLException {
		userName = resultSet.getString(1);
		count = resultSet.getInt(2);
	}

	public void write(DataOutput out) throws IOException {
	}

	public void readFields(DataInput in) throws IOException {
	}
}
