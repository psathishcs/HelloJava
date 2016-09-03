package org.hello.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseConnector {
	public static void main(String[] args) throws IOException{
		// You need a configuration object to tell the client where to connect.
		// When you create a HBaseConfiguration, it reads in whatever you've set
		// into your hbase-site.xml and in hbase-default.xml, as long as these
		// can be found on the CLASSPATH
		Configuration config = HBaseConfiguration.create();
		
		// This instantiates an HTable object that connects you to the
		// "myLittleHBaseTable" table.
		HTable hTable = new HTable(config, "myLittleHBaseTable");
		
		Put p = new Put(Bytes.toBytes("myLittleRow"));
		p.add(Bytes.toBytes("myLittleFamily"), Bytes.toBytes("sameQualifier"), Bytes.toBytes("some Value"));
		
		hTable.put(p);
		
		Get g = new Get(Bytes.toBytes("myLittleRow"));
		
		Result r = hTable.get(g);
		
		byte[] value = r.getValue(Bytes.toBytes("myLittleFamily"), Bytes.toBytes("sameQualifier"));
		String valueStr = Bytes.toString(value);
		System.out.println("Get : " + valueStr );
		
		// Sometimes, you won't know the row you're looking for. In this case,
		// you use a Scanner. This will give you cursor-like interface to the
		// contents of the table. To set up a Scanner, do like you did above
		// making a Put and a Get, create a Scan. Adorn it with column names,
		// etc.
		
		Scan s = new Scan();
		s.addColumn(Bytes.toBytes("myLittleFamily"), Bytes.toBytes("sameQualifier"));
		ResultScanner resultScanner = hTable.getScanner(s);
		
		try {
			for (Result result = resultScanner.next(); result != null; result =  resultScanner.next())
			{
				System.out.println(result);
			}
		}finally {
			resultScanner.close();
		}
		
		
		
		
	}
}
