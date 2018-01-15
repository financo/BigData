package com.wzy.fof;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class FofMapperOne extends Mapper<LongWritable, Text, Text, IntWritable>{

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] strs = StringUtils.split(value.toString(), ' ');
		Fof fof = new Fof();
		
		for (int i = 0; i < strs.length; i++) {
			
			String s1 = fof.format(strs[0], strs[i]);
			context.write(new Text(s1), new IntWritable(0));
			
			for (int j = i+1; j < strs.length; j++) {
				String s2 = fof.format(strs[i], strs[j]);
				context.write(new Text(s2), new IntWritable(1));
			}
		}
	}

}
