package com.wzy.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FirstJob {
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		 conf.set("fs.defaultFS", "hdfs://slave100:9000");
		 conf.set("yarn.resourcemanager.hostname", "slave100");
		// conf.set("mapred.jar", "C:\\Users\\Mr.w\\Desktop\\wc.jar");

		 try {
			FileSystem fs = FileSystem.get(conf);
			Job job = Job.getInstance(conf);
			job.setJarByClass(FirstJob.class);
			job.setJobName("weibo");
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			job.setNumReduceTasks(4);
			job.setPartitionerClass(FirstPartition.class);
			job.setMapperClass(FirstMapper.class);
			job.setCombinerClass(FirstReducer.class);
			job.setReducerClass(FirstReducer.class);
			
			FileInputFormat.addInputPath(job, new Path("/user/tdidf/input"));
			
			Path outpath = new Path("/user/tdidf/output1");
			if (fs.exists(outpath)) {
				fs.delete(outpath, true);
			}
			FileOutputFormat.setOutputPath(job, outpath);
			
			boolean flag = job.waitForCompletion(true);
			if (flag) {
				//TODO
				System.out.println("job success!");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
