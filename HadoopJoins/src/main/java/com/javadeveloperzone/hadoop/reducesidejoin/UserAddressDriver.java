package com.javadeveloperzone.hadoop.reducesidejoin;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class UserAddressDriver extends Configured implements Tool {
	
	private static final String DATA_SEPERATOR=",";
	
	private static String inputPath1,inputPath2, outputPath, queueName;

	public int run(String[] args) throws Exception
	{
		
		Configuration configuration = new Configuration();
		
		configuration.set("mapreduce.output.textoutputformat.separator",DATA_SEPERATOR);
		
//		configuration.set("mapreduce.job.queuename", queueName);
		
		Job job = Job.getInstance(configuration);
		
		job.setJobName("Reduce Side join");
		
		job.setJarByClass(UserAddressDriver.class);
		
		//Map
		
		job.setMapOutputKeyClass(LongWritable.class);
		
		job.setMapOutputValueClass(Text.class);
		
		//Job
		
		job.setOutputKeyClass(LongWritable.class);
		
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setReducerClass(UserDataReducer.class);
		
		MultipleInputs.addInputPath(job, new Path(inputPath1), TextInputFormat.class,UserFileMapper.class);
		
		MultipleInputs.addInputPath(job, new Path(inputPath2), TextInputFormat.class,AddressFileMapper.class);
		
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.waitForCompletion(true);

		return 0;
		
	}
	
	public static void main(String[] args) throws Exception 
	{
		
		if(args.length == 1){
			
			initializePropertiesFromPropertyFile(args[0]);
	
		int result = ToolRunner.run(new Configuration(), new UserAddressDriver(), args);
		
		if (0 == result) {
			System.out.println("Job Completed Successfully...");

		} else {
			System.out.println("Job Failed...");

		}
		}
		else{
			System.out.println("USAGE <PropertyFile>");
		}
		
	}
	
	private static void initializePropertiesFromPropertyFile(String sheetName) {

		try {

			BufferedReader reader = new BufferedReader(new FileReader(new File(
					sheetName)));

			String line;

			while ((line = reader.readLine()) != null) {

				// Skipping comment lines
				if (!line.startsWith("##") && line.length() > 0) {

					String key;

					if (line.contains("=")) {

						key = line.split("=")[0];

						if (key.equalsIgnoreCase("INPUT_PATH1")) {
							inputPath1 = line.split("=")[1];
						}
						else if(key.equalsIgnoreCase("INPUT_PATH2")) {
							inputPath2 = line.split("=")[1];
						}
						else if (key.equalsIgnoreCase("OUTPUT_PATH")) {
							outputPath = line.split("=")[1];
						} else if (key.equalsIgnoreCase("QUEUE")) {
							queueName = line.split("=")[1];
						}

					}

				}

			}
			reader.close();

		} catch (Exception exception) {

			exception.printStackTrace();

		}

	}

}
