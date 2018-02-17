package com.javadeveloperzone.bigdata.hbase.bulkdelete;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.NoServerForRegionException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.IdentityTableReducer;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HBaseBulkDeleteRowsDriver extends Configured implements Tool {

	
	public int run(String[] args) throws Exception {

		try {

			String tableName = args[1];
			String outputPath = args[0] + "/" + tableName + "/HFiles";

			Configuration configuration = getConf();
			configuration.set("HBaseReferenceTable", tableName);
			configuration.set("columnFamilies",
					getColumnFamilies(configuration, tableName));

			Job job = Job.getInstance(configuration, "HBase Table Bulk Delete");
			job.setJarByClass(HBaseBulkDeleteRowsDriver.class);

			job.setMapperClass(HBaseBulkDeleteRowsMapper.class);
			job.setReducerClass(IdentityTableReducer.class);
			job.setMapOutputKeyClass(ImmutableBytesWritable.class);
			job.setMapOutputValueClass(Put.class);
			job.setOutputKeyClass(ImmutableBytesWritable.class);
			job.setOutputValueClass(Put.class);

		
			String inputPath = args[2];
			FileInputFormat.setInputPaths(job, new Path(inputPath));

			HFileOutputFormat2.configureIncrementalLoad(job, new HTable(
					configuration, tableName));

			FileSystem.getLocal(getConf()).delete(new Path(outputPath), true);

			HFileOutputFormat2.setOutputPath(job, new Path(outputPath
					+ "/BulkLoadOutput"));

			boolean status = job.waitForCompletion(true);

			if (status) {

				LoadIncrementalHFiles loadFfiles = new LoadIncrementalHFiles(
						configuration);

				HTable hTable = new HTable(configuration, tableName);

				loadFfiles.doBulkLoad(new Path(outputPath), hTable);
				
				hTable.close();

				System.out.println("Job Completed...");
			} else {

				throw new IOException("error with job!");

			}

		} catch (NoServerForRegionException e) {
			e.printStackTrace();
			return 1;
		} catch (IOException e) {
			e.printStackTrace();
			return 1;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			return 1;
		} catch (Exception e) {
			e.printStackTrace();
			return 1;
		}

		return 0;
	}

	public static void main(String[] args) throws Exception {

		if (args.length == 3) {
			int exitCode = ToolRunner.run(HBaseConfiguration.create(),
					new HBaseBulkDeleteRowsDriver(), args);

			System.exit(exitCode);
		} else {
			System.out
					.println("Usage:<OutputPath> <HBaseTableName> <RowKeysInputFilePath>");

		}
	}

	/* Get Columnfamilies */
	private static String getColumnFamilies(Configuration configuration,
			String tableName) {
		List<String> columnFamilyList = new ArrayList<String>();
		try {
			HTable table = new HTable(configuration, tableName);
			Set<byte[]> columnFamilies = table.getTableDescriptor()
					.getFamiliesKeys();

			for (byte[] columnFamily : columnFamilies) {
				columnFamilyList.add(Bytes.toString(columnFamily));
			}
			table.close();

		} catch (Exception exception) {
			exception.printStackTrace();
		}
		System.out.println("ColumnFamily List" + columnFamilyList.toString());
		return columnFamilyList.toString().replace("[", "").replace("]", "")
				.replaceAll(" ", "");
	}

}