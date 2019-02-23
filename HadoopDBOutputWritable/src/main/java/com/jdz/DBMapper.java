package com.jdz;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DBMapper extends Mapper<LongWritable, DBInputWritable, Text, NullWritable> {


	protected void map(LongWritable id, DBInputWritable value, Context ctx) {
		try 
		{
			String userDetails = value.getUserName()+","+value.getDepartment();
			ctx.write(new Text(userDetails), NullWritable.get());

		} catch (IOException ioException) {
			ioException.printStackTrace();
		} catch (InterruptedException interruptedException) {
			interruptedException.printStackTrace();
		}
	}
}