package com.hadoop.myhadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MaxTempeatureMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

	private static final int MISSING = 9999;
	
	String rlg = "1111";
	
//	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String year = line.substring(15,19);
		int temp = Integer.parseInt(line);
		if ("1949".equals(year)) {
			context.write(new Text(year), new IntWritable(temp));
		}
	}

	@Override
	public void map(LongWritable arg0, Text arg1,
			OutputCollector<Text, IntWritable> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		
	}
}
