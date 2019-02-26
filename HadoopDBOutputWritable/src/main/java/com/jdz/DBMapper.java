package com.jdz;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DBMapper extends Mapper<LongWritable, Text, Text, Text> {


	@Override
	protected void map(LongWritable id, Text value, Context context) {
		try 
		{
			String[] productValues = value.toString().split(",");
			
			context.write(new Text(productValues[0]),value);	
		
		} catch (Exception exception) {
			exception.printStackTrace();
		}
	}
}