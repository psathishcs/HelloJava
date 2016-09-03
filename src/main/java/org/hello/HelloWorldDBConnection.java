package org.hello;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HelloWorldDBConnection {
	private static String url = "jdbc:oracle:thin:@ 192.168.1.6:1521:orcl";
	private static String userName = "scott";
	private static String password = "tiger";
	private static String sqlQuery = "select * from emp";
	public static void main(String[] args) throws ClassNotFoundException, SQLException{
		System.out.println("Hello DB Connection!");
		Class.forName("oracle.jdbc.driver.OracleDriver");
		Connection conn = DriverManager.getConnection( url, userName, password);
		Statement statement = conn.createStatement();
		ResultSet rs = statement.executeQuery(sqlQuery);
		while(rs.next()) {
			System.out.print(rs.getString(1) + "\t"); //
			System.out.print(rs.getString(2) + "\t");
			System.out.print(rs.getString(3) + "\t");
			System.out.print(rs.getString(4) + "\t");
			System.out.print(rs.getString(5) + "\t");
			System.out.print(rs.getString(6) + "\t");
			System.out.print(rs.getString(7) + "\t");
			System.out.print(rs.getString(8) + "\n");
		}
		conn.close();
	}
}
