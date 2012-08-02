package com.abloz.hbase;
//date:2012.6.7
//http://abloz.com
//hadoop 1.0.3
//hbase 0.94.0
//tested on centos 5.5
//cluster distributed system:Hadoop48,Hadoop47,Hadoop46
/*
 [zhouhh@Hadoop48 hbase-0.94.0]$ cat conf/hbase-site.xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
    <name>hbase.rootdir</name>
    <!-- value>file:///home/zhouhh/hbase</value -->
    <value>hdfs://Hadoop48:54310/hbase</value>
  </property>
<property>
    <name>hbase.cluster.distributed</name>
    <value>true</value>
</property>
<property>
    <name>hbase.master.port</name>
    <value>60000</value>
  </property>
<property>
      <name>hbase.zookeeper.quorum</name>
      <value>Hadoop48,Hadoop47,Hadoop46</value>
</property>

</configuration>

 */
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
//import org.apache.hadoop.hbase.client.Delete;
//import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.client.Action;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.KeyValue;

//import org.apache.hadoop.hbase.
public class HBaseMgr {
	//get configure of hbase-site.xml under classpath,so needn't any configuration any more.
	public static Configuration conf =  HBaseConfiguration.create();
	//or Configuration.set(String name, String value)
	/*
	 Configuration conf = new Configuration();
	 //same as from hbase-site.xml
	 conf.set("hbase.zookeeper.property.clientPort", "2181");  
     conf.set("hbase.zookeeper.quorum", "192.168.10.48,192.168.10.47,192.168.10.46");  
     public static Configuration conf1 = HBaseConfiguration.create(conf);
	 */
	
	public static void createTable(String tableName, String[] families) throws Exception
	{
		try
		{
			//table create,disable,exist ,drop,use HBaseAdmin
			HBaseAdmin hadmin = new HBaseAdmin(conf);
			if( hadmin.tableExists(tableName))
			{
				hadmin.disableTable(tableName);
				hadmin.deleteTable(tableName);
				System.out.println("table "+tableName+" exist,delete it.");
				
				
			}
			
			HTableDescriptor tbdesc = new HTableDescriptor(tableName);
			
			for(String family : families)
			{
				
				tbdesc.addFamily(new HColumnDescriptor(family));
			}
			
		
			hadmin.createTable(tbdesc);
		
		} catch (MasterNotRunningException e){
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		}
	
		System.out.println("table "+ tableName+ " create ok.");
		
	}
	
	public static void putData(String tableName,String rowKey,String family, String qualifier, String value ) throws Exception
	{
		//insert,update,delete,get row,column families, use HTable.
		try
		{
			if(qualifier == null) qualifier = "";
			if(value == null) value = "";
			
			HTable htb = new HTable(conf,tableName);
			Put put = new Put(rowKey.getBytes());
			
			put.add(family.getBytes(),qualifier.getBytes(),value.getBytes());
			htb.put(put);
			System.out.println("put data to "+ tableName + ",rowKey:"+rowKey+",family:"+family+",qual:"+qualifier+",value:"+value);
		}
		catch (IOException e) 
		{
		
			e.printStackTrace();
		}
	}
	
	public static void getData(String tableName, String rowKey) throws Exception
	{
		try
		{
			HTable htb = new HTable(conf,tableName);
			Get get = new Get(rowKey.getBytes());
			Result rs = htb.get(get);
			System.out.println("get from "+tableName+ ",rowkey:"+rowKey);
			for(KeyValue kv:rs.raw())
			{
				System.out.println(new String(kv.getRow()) +":\t"+
						new String(kv.getFamily())+":"+
						new String(kv.getQualifier())+",\t"+
						new String(kv.getValue())+",\t"+
						kv.getTimestamp()
						
						);
			}
			
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		
	}
	
	public static void scanData(String tableName) throws Exception
	{
		try
		{
			HTable htb = new HTable(conf,tableName);
			Scan scan = new Scan(tableName.getBytes());
			ResultScanner rss = htb.getScanner(scan);
			System.out.println("scan "+tableName);
			System.out.println("==============begin=================");
			for(Result r:rss)
			{
			
				for(KeyValue kv: r.raw())
				{
					System.out.println(new String(kv.getRow()) +":\t"+
							new String(kv.getFamily())+":"+
							new String(kv.getQualifier())+",\t"+
							new String(kv.getValue())+",\t"+
							kv.getTimestamp()		
									
							);
				}
			}
			System.out.println("================end===============");
			
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
	}
	
	public static void test_student()
	{
		String tableName = "student";
		//String[] families = {"age","sex"};
		
		String rowKey="1";
		String family="class";
		String token = "";
		//String[] tokens={"class","score"};;
		String value="";
		String[] families = {"class"};
		String[][] data={{"jenny", "chinese", "85"},
				{"jenny", "math", "55"},
				{"jenny", "english", "65"},
				{"alice", "chinese", "74"},
				{"alice", "math", "88"},
				{"alice", "english", "85"},
				{"kevin", "chinese", "35"},
				{"kevin", "math", "95"},
				{"kevin", "english", "75"}};
		try
		{
			HBaseMgr.createTable(tableName,families);
			
			for(String[] user:data)
			{
				rowKey=user[0];
				token = user[1];
				value = user[2];
						
				HBaseMgr.putData(tableName, rowKey, family, token, value);
				
			}
			
			
			
			HBaseMgr.getData(tableName, rowKey);
			
			HBaseMgr.scanData(tableName);
			
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	public static void test_people()
	{
		String tableName = "people";
		String rowKey="1";
		String family="";
		//String token="";
		String value="";
		String[] families = {"attribute"};
		String[][] data={{"1", "jenny", "jenny@example.com", "867-5309"},
				{"2", "alice", "alice@example.com", "555-1234"},
				{"3", "kevin", "kevinpet@example.com", "555-1212"}};
		
		try
		{
			HBaseMgr.createTable(tableName,families);
			
			for(String[] user:data)
			{
				rowKey=user[0];
				family="attribute";
				value=user[1];
				HBaseMgr.putData(tableName, rowKey, family, "name", value);
				value=user[2];
				HBaseMgr.putData(tableName, rowKey, family, "email", value);
				value=user[3];
				HBaseMgr.putData(tableName, rowKey, family, "phone", value);
				
			}
			
			
			HBaseMgr.getData(tableName, rowKey);
			
			HBaseMgr.scanData(tableName);
			
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}	
	
	public static void main(String[] args)
	{
		//test_people();
		test_student();
	}
	
	
}
