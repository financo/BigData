package com.wzy.tfidf;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LastReducer extends Reducer<Text, Text, Text, Text>{

	@Override
	protected void reduce(Text text, Iterable<Text> iterable, Context context)
			throws IOException, InterruptedException {
		StringBuffer sb = new StringBuffer();
		for (Text i : iterable) {
			sb.append(i.toString() + "\t");
		}
		context.write(text, new Text(sb.toString()));
	}

}
