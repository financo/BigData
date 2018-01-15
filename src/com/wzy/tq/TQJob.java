package com.wzy.tq;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/*
 * 找出每个月气温最高的两天，按照时间升序，温度降序排列
 * 数据格式如下：每行一条记录，以制表符分隔
 * 1949-10-01 14:21:02	34c
 */
public class TQJob {
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set("mapreduce.app-submission.cross-platform", "true");

//		conf.set("fs.defaultFS", "hdfs://slave101:8020");
//		conf.set("yarn.resourcemanager.hostname", "slave101");
		conf.set("mapred.jar", "C:\\Users\\Mr.w\\Desktop\\tq.jar");

		Job job = Job.getInstance(conf);
		job.setJarByClass(TQJob.class);

		job.setMapperClass(TQMapper.class);
		job.setMapOutputKeyClass(Weather.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(TQReducer.class);
		
		job.setPartitionerClass(TQPartition.class);
		job.setSortComparatorClass(TQSort.class);
		job.setGroupingComparatorClass(TQGroup.class);
		
		job.setNumReduceTasks(3);
		
		FileInputFormat.addInputPath(job, new Path("/user/tq/input"));

		Path outpath = new Path("/user/tq/output");
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
