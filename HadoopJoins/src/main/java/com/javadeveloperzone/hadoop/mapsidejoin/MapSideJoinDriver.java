package com.javadeveloperzone.hadoop.mapsidejoin;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MapSideJoinDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		Configuration configuration = new Configuration();

		Job job = Job.getInstance(configuration, "Map-side join with text lookup file in DistributedCache");

		job.addCacheFile(new URI(args[0]));

		job.setJarByClass(MapSideJoinDriver.class);

		FileInputFormat.setInputPaths(job, new Path(args[1]));

		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setMapperClass(MapSideJoinMapper.class);

		job.setNumReduceTasks(0);

		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;

	}

	public static void main(String[] args) {
		try {

			if (args.length == 3) {

				int result = ToolRunner.run(new MapSideJoinDriver(), args);

				if (0 == result) {
					System.out.println("Job Completed Successfully...");

				} else {
					System.out.println("Job Failed...");

				}

			} else {
				System.out.printf("Usage <LookUp_Reference_FilePath><InputPath><OutputPath>");
			}

		} catch (Exception exception) {
			exception.printStackTrace();
		}
	}

}
