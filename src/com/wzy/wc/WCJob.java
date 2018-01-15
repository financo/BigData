package com.wzy.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/*
 * 统计单词出现的次数
 * 数据格式如下：每行若干单词，空格符分割
 * hadoop hello world
 */
public class WCJob {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://slave100:9000");
		conf.set("yarn.resourcemanager.hostname", "slave100");
//		conf.set("mapreduce.app-submission.cross-platform", "true");
//		conf.set("mapred.jar", "C:\\Users\\Mr.w\\Desktop\\wc.jar");
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(WCJob.class);
		
		job.setMapperClass(WCMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(WCReducer.class);
		FileInputFormat.addInputPath(job, new Path("/user/wordcount/input"));
		
		Path outpath = new Path("/user/wordcount/output");
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
