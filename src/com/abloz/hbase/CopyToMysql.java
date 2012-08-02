package com.abloz.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@SuppressWarnings("deprecation")
public class CopyToMysql extends Configured implements Tool {
	private static final Log LOG = LogFactory.getLog(CopyToMysql.class);
	public static final String driverClassName = "com.mysql.jdbc.Driver";
	public static final String URL = "jdbc:mysql://Hadoop48/toplists";
	public static final String USERNAME = "root";//mysql username
	public static final String PASSWORD = "";//mysql password
	private static final String tableName="myaward";
	
	private Connection connection;
	

	   public static class AwardInfoRecord implements Writable,  DBWritable {
	     String userid;

	     String nick;
	     String loginid;
	     public AwardInfoRecord() {

	     }
	     public void readFields(DataInput in) throws IOException {
		        this.userid = Text.readString(in);
		        this.nick = Text.readString(in);
		        this.loginid = Text.readString(in);
		     }
		     public void write(DataOutput out) throws IOException {
		        Text.writeString(out,this.userid);
		        Text.writeString(out, this.nick);
		        Text.writeString(out, this.loginid);
		     }
		     public void readFields(ResultSet result) throws SQLException {
		        this.userid = result.getString(1);
		        this.nick = result.getString(2);
		        this.loginid = result.getString(3);
		     }
		     public void write(PreparedStatement stmt) throws SQLException {
		        stmt.setString(1, this.userid);

		        stmt.setString(2, this.nick);
		        stmt.setString(3, this.loginid);
		     }
	     public String toString() {
	        return new String(this.userid + " " +  this.nick +" " +this.loginid);
	     }
		
		
	   }
	public static Configuration conf;

	public static class MyMapper extends MapReduceBase implements Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, ImmutableBytesWritable> {
		@Override
		public void map(ImmutableBytesWritable key,	Result rs,
				OutputCollector<ImmutableBytesWritable, ImmutableBytesWritable> output, Reporter report) throws IOException
				{

				String rowkey = new String(key.get());
				
				String userid = new String(rs.getValue("info".getBytes(), "UserId".getBytes()));
				String nick = new String(rs.getValue("info".getBytes(), "nickName".getBytes()),HConstants.UTF8_ENCODING);
				String loginid = new String(rs.getValue("info".getBytes(), "loginId".getBytes()));
				
				output.collect(new ImmutableBytesWritable(userid.getBytes()),new ImmutableBytesWritable((nick+","+loginid).getBytes()));
				
				//LOG.info("map: userid:"+userid+",nick:"+nick);
			}

		@Override
		public void configure(JobConf job) {
			super.configure(job);
		}
		
	}
	public static class MyReducer extends MapReduceBase implements Reducer<ImmutableBytesWritable, ImmutableBytesWritable,AwardInfoRecord, Text>{
		
		@Override
		public void configure(JobConf job) {
			
			super.configure(job);
		}

		@Override
		public void reduce(ImmutableBytesWritable key,
				Iterator<ImmutableBytesWritable> it,
				OutputCollector<AwardInfoRecord, Text> output, Reporter report)
				throws IOException {
			AwardInfoRecord record = new AwardInfoRecord();
			record.userid=new String(key.get());
			String info = new String(it.next().get());
			record.nick = new String(info.split(",")[0]);
			record.loginid = new String(info.split(",")[1]);
			//LOG.debug("reduce: userid:"+record.userid+",nick:"+record.nick);
			output.collect(record, new Text());
		}
		
	}


	public static void main(String[] args) throws Exception {
		conf = HBaseConfiguration.create();
	
		int ret = ToolRunner.run(conf, new CopyToMysql(), args);
		System.exit(ret);

	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		createConnection(driverClassName, URL);

		JobControl control = new JobControl("mysql");
		JobConf job = new JobConf(conf,CopyToMysql.class);
		
		job.setJarByClass(CopyToMysql.class);
		String fromTable = "award_test";
		
		job.set("mapred.input.dir", fromTable);
		
		job.set("hbase.mapred.tablecolumns", "info:UserId info:nickName info:loginId");
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(ImmutableBytesWritable.class);
		job.setInputFormat(TableInputFormat.class);
		DBConfiguration.configureDB(job, driverClassName, URL, USERNAME, PASSWORD);
		String[] fields = {"userid","nick","loginid"};
		DBOutputFormat.setOutput(job, tableName, fields);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setNumReduceTasks(1);
		
		Job controlJob = new Job(job);
		
		control.addJob(controlJob);
				
		//JobClient.runJob(job);
		
		//control.run();
		Thread theController = new Thread(control);
		theController.start();
		//final
		while(!control.allFinished()){
			Thread.sleep(3000);
			System.out.print(".");
		}
		control.stop();
	
		System.out.println();
		LOG.info("job end!");
		return 0;
	}

	
	



	//connect
	private void createConnection(String driverClassName, String url) throws Exception {
		    Class.forName(driverClassName);
		    connection = DriverManager.getConnection(url,USERNAME,PASSWORD);
		    connection.setAutoCommit(false);
		  }

	//create table fast
	private void createTable(String tableName) throws SQLException {

	    String createTable = 
	      "CREATE TABLE " +tableName+
	      " (userid  VARCHAR(9) NOT NULL," +
	          
	            " nick VARCHAR(20) NOT NULL, " +
	            " loginid VARCHAR(20) NOT NULL, " +
	            " PRIMARY KEY (userid, caldate))";
	    Statement st = connection.createStatement();
	    try {
	      st.executeUpdate(createTable);
	      connection.commit();
	    } catch (Exception e) {
	    	LOG.warn("table '"+tableName+"' is already exist! so we do anything");
		}
	    finally {
	      st.close();
	    }
	  }
	//init 
//	private void initialize() throws Exception {
//		    if(!this.initialized) {
//		      createConnection(driverClassName, URL);
////		      dropTables(tableName);
//		      createTable(tableName);
//		      
//		      System.out.println("------------------create ----------------------");
//		      
//		      
//		      this.initialized = true;  
//		    }
//	}
}


