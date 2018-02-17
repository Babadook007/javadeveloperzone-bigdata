package com.javadeveloperzone.bigdata.hbase.bulkdelete;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HBaseBulkDeleteRowsMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>
{

	private List<byte[]> columnFamilyList;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		super.setup(context);
		
		Configuration configuration = context.getConfiguration();
		
		String columnFamilyLine = configuration.get("columnFamilies");
		
		String[] columnFamilies = new String[10];
		
		columnFamilyList = new ArrayList<byte[]>();
		
		if(columnFamilyLine != null && !columnFamilyLine.isEmpty() && columnFamilyLine.contains(","))
		{

			columnFamilies = configuration.get("columnFamilies").split(",");
			
			for (String columnFamily : columnFamilies)
			{
				columnFamilyList.add(Bytes.toBytes(columnFamily));

			}
			
		}
		else
		{
			columnFamilyList.add(Bytes.toBytes(columnFamilyLine));
		}
		
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		Put put = new Put(value.getBytes());
		for (byte[] columnFamily : columnFamilyList)
		{

			/*context.write(new ImmutableBytesWritable(value.getBytes()), new KeyValue(Bytes.toBytes(value.toString()), columnFamily, null, HConstants.LATEST_TIMESTAMP,
					KeyValue.Type.DeleteFamily));*/
			
			put.add(new KeyValue(Bytes.toBytes(value.toString()), columnFamily, null, HConstants.LATEST_TIMESTAMP,
					KeyValue.Type.DeleteFamily));

		}
		context.write(new ImmutableBytesWritable(put.getRow()), put);
		
	}
}