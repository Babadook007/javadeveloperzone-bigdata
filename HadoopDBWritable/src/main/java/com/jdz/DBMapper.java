package com.jdz;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DBMapper extends
		Mapper<LongWritable, DBInputWritable, Text, IntWritable> {
	private IntWritable one = new IntWritable(1);

	protected void map(LongWritable id, DBInputWritable value, Context ctx) {
		try {
			String[] keys = value.getUserName().split(" ");

			for (String key : keys) {
				ctx.write(new Text(key), one);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}