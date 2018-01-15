package com.wzy.hdfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sun.org.apache.bcel.internal.generic.NEW;
/**
 * src下需要放置core-site.xml	hdfs-site.xml文件，用于找到hdfs服务
 * @author wzy
 *
 */
public class HdfsDemo {
	
	Configuration conf = new Configuration();
	FileSystem fs;
	
	@Before
	public void begin() throws IOException {
		fs = FileSystem.get(conf);
	}
	
	@After
	public void end() {
		try {
			fs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void mkdir() throws Exception {
		Path path = new Path("/hdfs");
		if (fs.exists(path)) {
			fs.delete(path, true);
		}
		fs.mkdirs(path);
	}
	
	@Test
	public void upload() throws IOException {
		Path path = new Path("/hdfs/test");
		FSDataOutputStream outputStream = fs.create(path);
		FileUtils.copyFile(new File("C:\\Users\\Mr.w\\Desktop\\pagerank.txt"), outputStream);
	}
	
	@Test
	public void list() throws FileNotFoundException, IOException {
		Path path = new Path("/hdfs");
		FileStatus[] fss = fs.listStatus(path);
		for (FileStatus fileStatus : fss) {
			System.out.println(fileStatus.getPath() +"-"+ fileStatus.getLen() +"-"+ fileStatus.getAccessTime());
		}
	}
	
	@Test
	public void delete() throws IOException {
		Path path = new Path("/hdfs/test");
		fs.delete(path, true);
	}
	
	@Test
	public void upload2() throws IOException {
		Path path = new Path("/hdfs/test2");
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path, Text.class, Text.class);
		File file = new File("C:\\Users\\Mr.w\\Desktop\\test");
		for (File f : file.listFiles()) {
			writer.append(new Text(f.getName()), new Text(FileUtils.readFileToString(f)));
		}
	}
	
	@Test
	public void download2() throws IOException {
		Path path = new Path("/hdfs/test2");
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
		Text key = new Text();
		Text value = new Text();
		while (reader.next(key, value)) {
			System.out.println(key);
			System.out.println(value);
			System.out.println("---------------------------");
		}
	}
	
	
	
	
}
