package org.hello.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;


public class HelloHBase {
	public static void main(String[] args) throws IOException{
		Configuration  config = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(config);
		HTableDescriptor[] descriptors = admin.listTables();
		
		for (int i=0; i < descriptors.length; i++){
			System.out.println(descriptors[i].getNameAsString());
		}
		try {
			HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf("employees"));
			
			descriptor.addFamily(new HColumnDescriptor("personal"));
			descriptor.addFamily(new HColumnDescriptor("professional"));
			
			admin.createTable(descriptor);
			
			System.out.println("employees Table Created!");
			
			descriptors = admin.listTables();
			System.out.println("HBase :List of Table ");
			for (int i=0; i < descriptors.length; i++){
				System.out.println(descriptors[i].getNameAsString());
			}

			
		}catch(Exception exception){
			System.out.println("employees already Exit!");
		}

		
		
	    HTable hTable = new HTable(config, "employees");
		
		Put put = new Put(Bytes.toBytes("row1"));
		put.add(Bytes.toBytes("personal"), Bytes.toBytes("name"), Bytes.toBytes("Bharathi Baskar"));
		put.add(Bytes.toBytes("personal"), Bytes.toBytes("city"), Bytes.toBytes("Edison"));
		
		
		put.add(Bytes.toBytes("professional"), Bytes.toBytes("designation"), Bytes.toBytes("Manager"));
		put.add(Bytes.toBytes("professional"), Bytes.toBytes("salary"), Bytes.toBytes("85000"));
		hTable.put(put);
		
		
		put = new Put(Bytes.toBytes("row2"));
		put.add(Bytes.toBytes("personal"), Bytes.toBytes("name"), Bytes.toBytes("Sakthi Manoj"));
		put.add(Bytes.toBytes("personal"), Bytes.toBytes("city"), Bytes.toBytes("Portage"));
		
		put.add(Bytes.toBytes("professional"), Bytes.toBytes("designation"), Bytes.toBytes("Software Developer"));
		put.add(Bytes.toBytes("professional"), Bytes.toBytes("salary"), Bytes.toBytes("75000"));
		hTable.put(put);
		
		put = new Put(Bytes.toBytes("row3"));
		put.add(Bytes.toBytes("personal"), Bytes.toBytes("name"), Bytes.toBytes("Vijayalakshmi Jothi"));
		put.add(Bytes.toBytes("personal"), Bytes.toBytes("city"), Bytes.toBytes("Chennai"));
		
		put.add(Bytes.toBytes("professional"), Bytes.toBytes("designation"), Bytes.toBytes("Tester"));
		put.add(Bytes.toBytes("professional"), Bytes.toBytes("salary"), Bytes.toBytes("65000"));
		hTable.put(put);
		
		put = new Put(Bytes.toBytes("row4"));
		put.add(Bytes.toBytes("personal"), Bytes.toBytes("name"), Bytes.toBytes("Tamilmanam Senthilkumar"));
		put.add(Bytes.toBytes("personal"), Bytes.toBytes("city"), Bytes.toBytes("Charlotte"));
		
		put.add(Bytes.toBytes("professional"), Bytes.toBytes("designation"), Bytes.toBytes("Manager"));
		put.add(Bytes.toBytes("professional"), Bytes.toBytes("salary"), Bytes.toBytes("85000"));
		hTable.put(put);
		
		put = new Put(Bytes.toBytes("row5"));
		put.add(Bytes.toBytes("personal"), Bytes.toBytes("name"), Bytes.toBytes("Sathish Kumar Parthasarathy"));
		put.add(Bytes.toBytes("personal"), Bytes.toBytes("city"), Bytes.toBytes("Overland Park"));
		
		put.add(Bytes.toBytes("professional"), Bytes.toBytes("designation"), Bytes.toBytes("Senior Software Engineer"));
		put.add(Bytes.toBytes("professional"), Bytes.toBytes("salary"), Bytes.toBytes("115000"));
		hTable.put(put);
		
		
		hTable.close();
		
	}
}
