package com.wzy.fof;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/*
 * 二度关系
 * 输入数据如下：以空格分隔字符串，每行第一个代表一个账户，后边的为该账户的好友，找出二度关系以及亲密度（存在二度关系的个数）
 * tom hello hadoop cat
	world hadoop hello hive
	cat tom hive
	mr hive hello
	hive cat hadoop world hello mr
	hadoop tom hive world
	hello tom world hive mr
 */
public class FofJobOne {
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		 conf.set("fs.defaultFS", "hdfs://slave100:9000");
		 conf.set("yarn.resourcemanager.hostname", "slave100");
		// conf.set("mapred.jar", "C:\\Users\\Mr.w\\Desktop\\wc.jar");

		Job job = Job.getInstance(conf);
		job.setJarByClass(FofJobOne.class);

		job.setMapperClass(FofMapperOne.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(FofReducerOne.class);
		FileInputFormat.addInputPath(job, new Path("/user/fof/input"));

		Path outpath = new Path("/user/fof/output1");
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
