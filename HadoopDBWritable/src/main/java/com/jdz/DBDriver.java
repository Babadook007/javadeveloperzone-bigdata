package com.jdz;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DBDriver extends Configured implements Tool{

	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
	     DBConfiguration.configureDB(conf,
	     "com.mysql.jdbc.Driver",   // driver class
	     "jdbc:mysql://localhost:3306/pmc_maverick_dev", // db url
	     "root",    // user name
	     "root"); //password

	     Job job = Job.getInstance(conf);
	     job.setJarByClass(DBDriver.class);
	     job.setMapperClass(DBMapper.class);
	     job.setReducerClass(DBReducer.class);
	     job.setMapOutputKeyClass(Text.class);
	     job.setMapOutputValueClass(IntWritable.class);
	     job.setOutputKeyClass(DBOutputWritable.class);
	     job.setOutputValueClass(NullWritable.class);
	     job.setInputFormatClass(DBInputFormat.class);
	     job.setOutputFormatClass(DBOutputFormat.class);

	     DBInputFormat.setInput(
	     job,
	     DBInputWritable.class,
	     "users",   //input table name
	     null,
	     null,
	     new String[] { "user_id", "user_name" }  // table columns
	     );

	     DBOutputFormat.setOutput(
	     job,
	     "unique_users",    // output table name
	     new String[] { "user_name", "count" }   //table columns
	     );

	     System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}
	
	public static void main(String[] args) {
		
		try{
		
		int result = ToolRunner.run(new Configuration(), new DBDriver(), args);
		
		System.out.println("job status ::"+result);
		}
		catch(Exception exception)
		{
			exception.printStackTrace();
		}
			
	}
}
