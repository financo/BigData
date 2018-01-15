package com.wzy.fof;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FofJobTwo {
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		conf.set("fs.defaultFS", "hdfs://slave100:9000");
		conf.set("yarn.resourcemanager.hostname", "slave100");
		// conf.set("mapred.jar", "C:\\Users\\Mr.w\\Desktop\\wc.jar");

		Job job = Job.getInstance(conf);
		job.setJarByClass(FofJobTwo.class);

		job.setMapperClass(FofMapperTwo.class);
		job.setMapOutputKeyClass(Friend.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(FofReducerTwo.class);
		
		job.setSortComparatorClass(FofSort.class);
		job.setGroupingComparatorClass(FofGroup.class);
		
		FileInputFormat.addInputPath(job, new Path("/user/fof/output1"));

		Path outpath = new Path("/user/fof/output");
		FileSystem fs = FileSystem.get(conf);

		if (fs.exists(outpath)) {
			fs.delete(outpath, true);
		}

		FileOutputFormat.setOutputPath(job, outpath);

		boolean flag = job.waitForCompletion(true);

		if (flag) {
			System.out.println("job success!");
		}
	}
}