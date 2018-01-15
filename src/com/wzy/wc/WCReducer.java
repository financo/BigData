package com.wzy.wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	protected void reduce(Text text, Iterable<IntWritable> iter, Context context) 
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable i : iter) {
			sum += i.get();
		}
		context.write(text, new IntWritable(sum));
	}

}
