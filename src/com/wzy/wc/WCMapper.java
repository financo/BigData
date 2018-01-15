package com.wzy.wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String str = value.toString();
		String[] strings = StringUtils.split(str, ' ');
		for (String string : strings) {
			context.write(new Text(string), new IntWritable(1));
		}
	}
}
