package com.javadeveloperzone.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object,Text,Text,IntWritable>
{
	
	private static final IntWritable countOne = new IntWritable(1);
	
	private Text word = new Text();
	
	public void map(Object key, Text value, Context context) throws IOException,InterruptedException
	{
		String [] words = value.toString().split(" ");
		
		for(String string : words)
		{
			
			word.set(string);
			
			context.write(word, countOne);
			
		}
	}	

}
