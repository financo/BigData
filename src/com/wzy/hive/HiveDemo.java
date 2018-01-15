package com.wzy.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveDemo {
	
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	
	public static void main(String[] args) throws SQLException {
		try {
			Class.forName(driverName);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		Connection connection = DriverManager.getConnection("jdbc:hive2://slave101:10000/default", "wzy", "");
		Statement statement = connection.createStatement();
		
		String sql = "select * from test";
		System.out.println("Running: " + sql);
		ResultSet rs = statement.executeQuery(sql);
		while(rs.next()) {
			System.out.println(rs.getString(1) + "-" + rs.getString("name"));
		}
	}
}
