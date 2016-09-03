package org.hello.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
/**
 * 
 * @author Sathish
 *
 */
public class HBaseTest {
	private static Configuration conf = null;
	static {
		conf = HBaseConfiguration.create();
	}
	
	/**
	 *  Create a table
	 *  
	 * @param tableName
	 * @param familys
	 * @throws Exception
	 */
	public static void creatTable(String tableName, String[] familys) throws Exception
	{
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName)){
			System.out.println("table already exits!");
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			for (int i = 0; i< familys.length; i++) {
				tableDesc.addFamily(new HColumnDescriptor(familys[i])); 
			}
			admin.createTable(tableDesc);
			System.out.println("Create table " + tableName + " ok");
		}
	}
	
	/**
	 *  Deleting a table
	 *  
	 * @param tableName
	 * @throws Exception
	 */
	public static void deleteTalbe(String tableName) throws Exception 
	{
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			System.out.println("Deleted table " + tableName + " ok");
		}catch(MasterNotRunningException me){
			me.printStackTrace();
		}catch (ZooKeeperConnectionException ze){
			ze.printStackTrace();
		}
	}
	
	/**
	 *  Adding Record
	 *  
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @param value
	 * @throws Exception
	 */
	public static void addRecord(String tableName, String rowKey, String family, String qualifier, String value) throws Exception
	{
		try {
			HTable table = new HTable(conf, tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
			table.put(put);
			System.out.println("Insert recored " + rowKey + "to table " + tableName + " ok");
			
		}catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 *  Delete Record
	 *  
	 * @param tableName
	 * @param rowKey
	 * @throws IOException
	 */
	public static void delRecord(String tableName, String rowKey) throws IOException 
	{
		HTable table = new HTable(conf, tableName);
		List<Delete> list = new ArrayList<Delete>();
		Delete del = new Delete(rowKey.getBytes());
		list.add(del);
		table.delete(list);
		System.out.println("del Recored " + rowKey + " ok.");
	}
	
	/**
	 *  get One Record
	 *  
	 * @param tableName
	 * @param rowKey
	 * @throws IOException
	 */
	public static void getOneRecord(String tableName, String rowKey) throws IOException
	{
		HTable table = new HTable(conf, tableName);
		Get get = new Get(rowKey.getBytes());
		Result rs = table.get(get);
		for (KeyValue kv : rs.raw()){
			System.out.print(new String(kv.getRow()) + " ");
			System.out.print(new String(kv.getFamily()) + ":");
			System.out.print(new String(kv.getQualifier()) + " ");
			System.out.print(kv.getTimestamp() + " ");
			System.out.println(new String(kv.getValue()));
		}
	}
	
	public static void getAllRecord(String tableName) throws IOException 
	{
		HTable table = new HTable(conf, tableName);
		Scan scan = new Scan();
		ResultScanner resultScanner = table.getScanner(scan);
		for (Result result : resultScanner){
			for (KeyValue kv : result.raw()){
				System.out.print(new String(kv.getRow()) + " ");
				System.out.print(new String(kv.getFamily()) + ":");
				System.out.print(new String(kv.getQualifier()) + " ");
				System.out.print(kv.getTimestamp() + " ");
				System.out.println(new String(kv.getValue()));
			}
		}
	}
	
	public static void main(String[] args){
		try {
			String tableName = "scores";
			String[] familys = {"grade", "course"};
			
			HBaseTest.creatTable(tableName, familys);
			
			HBaseTest.addRecord(tableName, "zkb", "grade", "", "5");
			HBaseTest.addRecord(tableName, "zkb", "course", "", "90");
			HBaseTest.addRecord(tableName, "zkb", "course", "math", "97");
			HBaseTest.addRecord(tableName, "zkb", "course", "art", "87");
			
			HBaseTest.addRecord(tableName, "baoniu", "grade", "", "4");
			HBaseTest.addRecord(tableName, "baoniu", "course", "math", "89");
			
			System.out.println("==============get one record==============");
			HBaseTest.getOneRecord(tableName, "zkb");
			
			System.out.println("==============get all record==============");
			HBaseTest.getAllRecord(tableName);
			
			System.out.println("==============delete one record==============");
			HBaseTest.delRecord(tableName, "baoniu");
			HBaseTest.getAllRecord(tableName);
			
			System.out.println("==============get all record==============");
			HBaseTest.getAllRecord(tableName);
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
