package com.wzy.tq;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TQReducer extends Reducer<Weather, IntWritable, Text, NullWritable>{

	@Override
	protected void reduce(Weather weather, Iterable<IntWritable> iter, Context context)
			throws IOException, InterruptedException {
		int flag = 0;
		for (IntWritable i : iter) {
			flag++;
			if (flag>2) {
				break;
			}
			String msg = weather.getYear() +"-"+ weather.getMonth() +"-"+ weather.getDay() +"-"+ i.get();
			context.write(new Text(msg), NullWritable.get());
		}
	}

}
