package com.wzy.tfidf;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;


public class FirstPartition extends HashPartitioner{

	@Override
	public int getPartition(Object key, Object value, int numReduceTasks) {
		if (key.equals(new Text("count") )) {
			return 3;
		}else {
			return super.getPartition(key, value, numReduceTasks -1);
		}
	}
}
