package com.jdz;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DBReducer extends Reducer<Text,Text,DBOutputWritable,NullWritable>{
	
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, DBOutputWritable, NullWritable>.Context context) throws IOException, InterruptedException {
		
		Text value = values.iterator().next();
		
		String[] productValues = value.toString().split(",");
		
		DBOutputWritable productRecord = new DBOutputWritable(productValues[0], productValues[1], Integer.parseInt(productValues[2]));
		
		context.write(productRecord, NullWritable.get());

	}

}
