package com.wzy.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TwoJob {
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://slave100:9000");
		conf.set("yarn.resourcemanager.hostname", "slave100");
		// conf.set("mapred.jar", "C:\\Users\\Mr.w\\Desktop\\wc.jar");

		 try {
			Job job = Job.getInstance(conf);
			job.setJarByClass(TwoJob.class);
			job.setJobName("weibo2");
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			job.setMapperClass(TwoMapper.class);
			job.setCombinerClass(TwoReducer.class);
			job.setReducerClass(TwoReducer.class);
			
			FileInputFormat.addInputPath(job, new Path("/user/tdidf/output/weibo1"));
			FileOutputFormat.setOutputPath(job, new Path("/user/tfidf/output/weibo2"));
			
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
