package com.wzy.hBase;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
/**
 * src下需要放置log4j.properties文件
 * @author wzy
 *
 */
public class HBaseDemo {

	HBaseAdmin hBaseAdmin;
	HTable hTable;
	String TN = "phone";

	@Before
	public void begin() throws Exception {
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "slave102,slave103,slave104");
		hBaseAdmin = new HBaseAdmin(conf);
		hTable = new HTable(conf, TN);
	}

	@After
	public void end() {
		if (hBaseAdmin != null) {
			try {
				hBaseAdmin.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Test
	public void createTbl() throws IOException {
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(TN));
		HColumnDescriptor family = new HColumnDescriptor("cf1");

		family.setBlockCacheEnabled(true);
		family.setInMemory(true);
		family.setMaxVersions(1);

		desc.addFamily(family);

		hBaseAdmin.createTable(desc);
	}

	@Test
	public void insert() throws IOException {
		String rowkey = "17317289607_2017122221212";
		Put put = new Put(rowkey.getBytes());

		put.add("cf1".getBytes(), "type".getBytes(), "1".getBytes());
		put.add("cf1".getBytes(), "time".getBytes(), "2017".getBytes());
		put.add("cf1".getBytes(), "pnum".getBytes(), "13523127019".getBytes());

		hTable.put(put);
	}

	@Test
	public void get() throws IOException {
		String rowkey = "17317289607_2017122221212";
		Get get = new Get(rowkey.getBytes());

		get.addColumn("cf1".getBytes(), "type".getBytes());
		get.addColumn("cf1".getBytes(), "time".getBytes());

		Result rs = hTable.get(get);
		Cell cell = rs.getColumnLatestCell("cf1".getBytes(), "type".getBytes());
		System.out.println("===========" + new String(CellUtil.cloneValue(cell)));
	}
	
	Random r = new Random();
	
	private String getPhoneNum(String prefix) {
		return prefix + String.format("%08d", r.nextInt(99999999));
	}
	
	private String getDate(String year) {
		return year + String.format("%02d%02d%02d%02d%02d", 
				new Object[] {
						r.nextInt(12) + 1, r.nextInt(29) + 1, r.nextInt(60) + 1,
						r.nextInt(60) + 1, r.nextInt(60) + 1
				});
	}

	@Test
	public void insertDB() throws Exception {
		List<Put> puts = new ArrayList<Put>();
		for (int i = 0; i < 10; i++) {
			String rowkey;
			String phoneNum = getPhoneNum("186");
			for (int j = 0; j < 10; j++) {
				String phoneDate = getDate("2017");
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
				try {
					long dateLong = sdf.parse(phoneDate).getTime();
					rowkey = phoneNum + (Long.MAX_VALUE - dateLong);
					
					Put put = new Put(rowkey.getBytes());
					put.add("cf1".getBytes(), "type".getBytes(), (r.nextInt(2) + "").getBytes());
					put.add("cf1".getBytes(), "time".getBytes(), phoneDate.getBytes());
					put.add("cf1".getBytes(), "pnum".getBytes(), (getPhoneNum("177")).getBytes());
					puts.add(put);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		hTable.put(puts);
	}
	
	
	@Test
	public void scanDB() throws Exception {
		Scan scan = new Scan();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		
		String startRowkey = "18619928000" + (Long.MAX_VALUE - sdf.parse("20170401000000").getTime());
		scan.setStartRow(startRowkey.getBytes());
		
		String stopRowkey = "18619928000" + (Long.MAX_VALUE - sdf.parse("20170201000000").getTime());
		scan.setStopRow(stopRowkey.getBytes());
		
		ResultScanner rss = hTable.getScanner(scan);
		for (Result rs : rss) {
			System.out.println(new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf1".getBytes(), "type".getBytes()))) + "-"
					+ new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf1".getBytes(), "time".getBytes()))) + "-"
					+ new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf1".getBytes(), "pnum".getBytes()))));
		}
	}
	
	@Test
	public void scanDB2() throws Exception {
		FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		
		PrefixFilter prefixFileFilter = new PrefixFilter("18619928000".getBytes());
		list.addFilter(prefixFileFilter);
		
		SingleColumnValueFilter singleColumnValueFilter = 
				new SingleColumnValueFilter("cf1".getBytes(), "type".getBytes(), 
						CompareOp.EQUAL, "0".getBytes());
		list.addFilter(singleColumnValueFilter);
		
		Scan scan = new Scan();
		scan.setFilter(list);
		
		ResultScanner rss = hTable.getScanner(scan);
		for (Result rs : rss) {
			String rowkey = new String(rs.getColumnLatestCell("cf1".getBytes(), "type".getBytes()).getRow());
			System.out.println(rowkey + "-"
					+ new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf1".getBytes(), "type".getBytes()))) + "-"
					+ new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf1".getBytes(), "time".getBytes()))) + "-"
					+ new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf1".getBytes(), "pnum".getBytes()))));
		}
	}
	
	
	
	
	
	
	
}
