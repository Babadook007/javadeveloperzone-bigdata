package com.javadeveloperzone.hadoop.mapsidejoin;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapSideJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static HashMap<String, String> departmentMap = new HashMap<String, String>();
	private BufferedReader bufferedrReader;
	private String deptName = "";
	private Text txtMapOutputKey = new Text("");
	private Text txtMapOutputValue = new Text("");

	enum MyCounter {
		RecordsCount, FileExists, FileNotFound, OtherError
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		Path[] cacheFilesLocal = context.getLocalCacheFiles();
		
		for (Path uri : cacheFilesLocal) {
			
			if (uri.toString().trim().contains("department.txt")) {
				context.getCounter(MyCounter.FileExists).increment(1);
				loadDepartmentsHashMap(uri, context);
			}
		}

	}

	private void loadDepartmentsHashMap(Path filePath, Context context)
			throws IOException {

		String strLineRead = "";

		try {
			
			
			bufferedrReader = new BufferedReader(new FileReader(filePath.toString()));

			// Read each line, split and load to HashMap
			while ((strLineRead = bufferedrReader.readLine()) != null) {
				String deptFieldArray[] = strLineRead.split("\t");
				departmentMap.put(deptFieldArray[0].trim(),
						deptFieldArray[1].trim());
			}
		} catch (FileNotFoundException exception) {
			exception.printStackTrace();
			context.getCounter(MyCounter.FileNotFound).increment(1);
		} catch (IOException exception) {
			context.getCounter(MyCounter.OtherError).increment(1);
			exception.printStackTrace();
		} finally {
			if (bufferedrReader != null) {
				bufferedrReader.close();
			}
		}
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		context.getCounter(MyCounter.RecordsCount).increment(1);

		if (value.toString().length() > 0) {
			String arrEmpAttributes[] = value.toString().split("\t");

			try {
				deptName = departmentMap.get(arrEmpAttributes[3].toString());
			} finally {
				deptName = ((deptName.equals(null) || deptName
						.equals("")) ? "NOT-FOUND" : deptName);
			}

			txtMapOutputKey.set(arrEmpAttributes[0].toString());

			txtMapOutputValue.set(arrEmpAttributes[1].toString() + "\t"
					+ arrEmpAttributes[2].toString() + "\t"
					+ deptName);

		}
		context.write(txtMapOutputKey, txtMapOutputValue);
		deptName = "";
	}
}
