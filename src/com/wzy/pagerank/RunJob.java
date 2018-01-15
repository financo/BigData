package com.wzy.pagerank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/*
 * 计算网页的排名
 * 输入数据如下：每一行代表一组数据，以制表符分隔，每个代表一个页面，第一个页面中存在指向后面页面的链接
 * A	B	D
	B	C
	C	A	B
	D	B	C
 */
public class RunJob {

	public static enum Mycounter{
		my
	}
	
	public static void main(String[] args) {
		Configuration config = new Configuration();
//		config.set("fs.defaultFS", "hdfs://slave100:9000");
//		config.set("yarn.resourcemanager.hostname", "slave100");
		config.set("mapred.jar", "C:\\Users\\Mr.w\\Desktop\\pagerank.jar");
		config.set("mapreduce.app-submission.cross-platform", "true");
		double d = 0.001;
		int i = 0;
		while(true) {
			i++;
			try {
				config.setInt("runCount", i);
				FileSystem fs = FileSystem.get(config);
				Job job = Job.getInstance(config);
				job.setJarByClass(RunJob.class);
				job.setJobName("pr" + i);
				job.setMapperClass(PageRankMapper.class);
				job.setReducerClass(PageRankReducer.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);
				//对输入的一行数据进行格式化，以制表符分隔，第一个为key，后边为value
				job.setInputFormatClass(KeyValueTextInputFormat.class);
				//设置输入输出路径
				Path inputPath = new Path("/user/pagerank/input/pagerank.txt");
				if (i>1) {
					inputPath = new Path("/user/pagerank/output/pr" + (i - 1));
				}
				FileInputFormat.addInputPath(job, inputPath);
				Path outputPath = new Path("/user/pagerank/output/pr" + i);
				if (fs.exists(outputPath)) {
					fs.delete(outputPath, true);
				}
				FileOutputFormat.setOutputPath(job, outputPath);
				//提交任务
				boolean f = job.waitForCompletion(true);
				//判断作业是否完成
				if (f) {
					System.out.println("success.");
					long sum = job.getCounters().findCounter(Mycounter.my).getValue();
					System.out.println("SUM: " + sum);
					double avgd = sum / 4000.0;
					if (avgd < d) {
						break;
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	static class PageRankMapper extends Mapper<Text, Text, Text, Text> {
		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			int runCount = context.getConfiguration().getInt("runCount", 1);
			String page = key.toString();
			Node node = null;
			if (runCount == 1) {
				node = Node.fromMR("1.0" + "\t" + value.toString());
			}else {
				node = Node.fromMR(value.toString());
			}
			context.write(new Text(page), new Text(node.toString()));
			if (node.containsAdjacentNodes()) {
				double outValue = node.getPageRank() / node.getAdjacentNodeNames().length;
				for (int i = 0; i < node.getAdjacentNodeNames().length; i++) {
					String outPage = node.getAdjacentNodeNames()[i];
					context.write(new Text(outPage), new Text(outValue + ""));
				}
			}
		}
	}
	
	static class PageRankReducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text text, Iterable<Text> iterable, Context context)
				throws IOException, InterruptedException {
			double sum = 0.0;
			Node sourceNode = null;
			for (Text i : iterable) {
				Node node = Node.fromMR(i.toString());
				if (node.containsAdjacentNodes()) {
					sourceNode = node;
				}else {
					sum = sum + node.getPageRank();
				}
			}
			double newPR = (0.15 / 4.0) + (0.85 * sum);
			System.out.println("**************** new pageRank value is " + newPR + "********************");
			double d = newPR - sourceNode.getPageRank();
			int j = (int) (d * 1000.0);
			j = Math.abs(j);
			System.out.println(j + "--------------------");
			context.getCounter(Mycounter.my).increment(j);
			sourceNode.setPageRank(newPR);
			context.write(text, new Text(sourceNode.toString()));
		}
	}
}
