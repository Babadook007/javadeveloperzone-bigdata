package com.javadeveloperzone.bigdata.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.javadeveloperzone.bigdata.hadoop.datatypes.DBOutputWritable;

public class DBReducer extends Reducer<Text ,IntWritable,DBOutputWritable,NullWritable>
{
	@Override
	protected void reduce(
			Text key,
			Iterable<IntWritable> values,
			Reducer<Text, IntWritable, DBOutputWritable, NullWritable>.Context context)
			throws IOException, InterruptedException {
		
		int total=0;
		
		for(IntWritable value : values)
		{
			total+=value.get();
		}
		
		context.write(new DBOutputWritable(key.toString(), total), NullWritable.get());
	}
}
