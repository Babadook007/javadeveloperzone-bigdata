package com.javadeveloperzone.hadoop.mapsidejoin;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
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

	private static String inputPath, outputPath, queueName,
			referenceDatasetURI;

	@Override
	public int run(String[] args) throws Exception {

		Configuration configuration = new Configuration();

		// configuration.set("mapreduce.job.queuename", queueName);

		Job job = Job.getInstance(configuration,
				"Map-side join with text lookup file in DistributedCache");

		job.addCacheFile(new URI(referenceDatasetURI));

		job.setJarByClass(MapSideJoinDriver.class);

		FileInputFormat.setInputPaths(job, new Path(inputPath));

		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setMapperClass(MapSideJoinMapper.class);

		job.setNumReduceTasks(0);

		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;

	}

	public static void main(String[] args) {
		try {

			if (args.length == 1) {
				/* initialize properties from file.. */

				initializePropertiesFromPropertyFile(args[0]);

				int result = ToolRunner.run(new MapSideJoinDriver(), args);

				if (0 == result) {
					System.out.println("Job Completed Successfully...");

				} else {
					System.out.println("Job Failed...");

				}

			} else {
				System.out.printf("Usage <Control Properties>");
			}

		} catch (Exception exception) {
			exception.printStackTrace();
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

						if (key.equalsIgnoreCase("INPUT_PATH")) {
							inputPath = line.split("=")[1];
						} else if (key.equalsIgnoreCase("OUTPUT_PATH")) {
							outputPath = line.split("=")[1];
						} else if (key
								.equalsIgnoreCase("REFERENCE_DATASET_URI")) {
							referenceDatasetURI = line.split("=")[1];
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
