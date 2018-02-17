package com.javadeveloperzone.bigdata.hadoop.datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

/*CREATE TABLE `pmc_maverick_dev`.`users` (
		  `user_id` INT NOT NULL AUTO_INCREMENT,
		  `user_name` VARCHAR(45) NOT NULL,
		  `department` VARCHAR(45) NOT NULL,
		  PRIMARY KEY (`user_id`));
*/

public class DBInputWritable implements Writable, DBWritable
{

	private int userId;
	
	private String userName,department;
	
	public void write(PreparedStatement statement) throws SQLException {
		statement.setInt(1, userId);
		statement.setString(2, userName);
		statement.setString(3, department);
		
	}

	public void readFields(ResultSet resultSet) throws SQLException {
		userId = resultSet.getInt(1);
		userName = resultSet.getString(2);
		department=resultSet.getString(3);
	}

	public void write(DataOutput out) throws IOException {
	}

	public void readFields(DataInput in) throws IOException {
	}

	public int getUserId() {
		return userId;
	}
	public String getUserName() {
		return userName;
	}
	public String getDepartment() {
		return department;
	}

}
