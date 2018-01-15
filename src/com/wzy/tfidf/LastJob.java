package com.wzy.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LastJob {
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://slave100:9000");
		conf.set("yarn.resourcemanager.hostname", "slave100");
		// conf.set("mapred.jar", "C:\\Users\\Mr.w\\Desktop\\wc.jar");

		 try {
			FileSystem fs = FileSystem.get(conf);
			 
			Job job = Job.getInstance(conf);
			job.setJarByClass(LastJob.class);
			job.setJobName("weibo3");
			
			job.addCacheFile(new Path("/user/tfidf/output/weibo1/part-r-00003").toUri());
			job.addCacheFile(new Path("/user/tfidf/output/weibo2/part-r-00000").toUri());
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			job.setMapperClass(LastMapper.class);
			job.setReducerClass(LastReducer.class);
			
			FileInputFormat.addInputPath(job, new Path("/user/tdidf/output/weibo1"));
			
			Path outpath = new Path("/user/tdidf/output/weibo3");
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
