package org.hello;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class HelloReadCassandra {
	public static void main(String[] args){
		Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		Session session = cluster.connect();
		System.out.println("Connected to the Cassandra NoSQL database......");
		
		session.execute("USE tp");
		String query = "SELECT * FROM emp";
		ResultSet resultSet =  session.execute(query);
		for(Row row : resultSet){
			System.out.print(row.getInt("emp_id") + "\t");
			System.out.print(row.getString("emp_name") + "\t");
			System.out.print(row.getString("emp_email") + "\t");
			System.out.print(row.getString("emp_city") + "\t");
			System.out.print(row.getVarint("emp_phone") + "\t");
			System.out.print(row.getVarint("emp_sal") + "\n");
		}
		

	}
}
