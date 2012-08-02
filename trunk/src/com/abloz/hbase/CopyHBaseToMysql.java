package com.abloz.hbase;

/*
 * @Author zhouhh
 * page: http://abloz.com
 * 
 */
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

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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
import java.util.Arrays;
@SuppressWarnings("deprecation")
public class CopyHBaseToMysql extends Configured implements Tool {
	private static final Log LOG = LogFactory.getLog(CopyHBaseToMysql.class);
	public static final String driverClassName = "com.mysql.jdbc.Driver";
	private static  String htable="";
	private static String hcolums = "info:UserId info:nickName info:loginId";
	
	public static  String mysqlurl = "";
	public static  String mysqlhost = "";
	public static  String mysqldb = "";
	public static  String username = "root";//mysql username
	public static  String passwd = "";//mysql password
	private static  String mtable="";
	
	private static String [] mfields={};
	
	private static String encoding=HConstants.UTF8_ENCODING;
	
	private static final String CMDLINE = "CopyHBaseToMysql [-h mysql_host] [-u mysql_user] [-p mysql_pass]  -ht hbase_table -hc hbase_colums " +
	                          "-md mysql_dbname -mt mysql_table -mf mysql_fields [-en encoding]";
	private Connection connection;
	public static Configuration conf;
	
	CopyHBaseToMysql(String[] args)
	{

	}

	private static int parseArgs(Configuration conf,String[] args)
	{
		LOG.info("args:"+ Arrays.toString(args));
		
		HelpFormatter help = new HelpFormatter();
        Options options = new Options();
        options.addOption("help", "help", false, "print program usage");
        options.addOption("h", "host", true, "mysql host name[:port] or ip[:port],default is 'localhost'");
        options.addOption("u", "user", true, "mysql user name, default is 'root'");
        options.addOption("p", "passwd", true, "mysql password of user name, default is ''");
        options.addOption("ht", "htable", true, "hbase table name to export data from ");
        options.addOption("hc", "hcolum", true, "space delimited list of hbase columns in colfam:colname format,eg. \"info:UserId info:nickName info:loginId\",seprated by ' '");
        options.addOption("mt", "mtable", true, "mysql table name to export data to");
        options.addOption("md", "mydb", true, "mysql database name");
        options.addOption("mf", "mfields", true, "mysql fields name ,such as \"userid,nick,loginid\",seprated by ',', should map with hc parameter");
        options.addOption("en", "encoding", true, "encoding of multibyte character set,default is \"UTF-8\"");
        // Space delimited list of columns in ColFam:ColName format (ColName can be ommitted to read all columns from a hbase table)
        CommandLineParser parser = new BasicParser();
        CommandLine cline;
        try {
            cline = parser.parse(options, args);
            String[] oargs = cline.getArgs();
            
            if (oargs.length < 5) {
                //help.printHelp(CMDLINE, options);
                //return -1;
            }
        } catch (ParseException e) {
            System.out.println(e);
            e.printStackTrace();
            help.printHelp(CMDLINE, options);
            return -1;
        }
 

        try {
            if (cline.hasOption('h'))
            	mysqlhost = cline.getOptionValue('h');
            else
            	mysqlhost = "localhost";
            if (cline.hasOption('u'))
                username = cline.getOptionValue('u');
            else
                username = "root";
            if (cline.hasOption('p'))
                passwd = cline.getOptionValue('p');
            else
                passwd = "";
            
            if (cline.hasOption("en"))
                encoding = cline.getOptionValue("en");
            else
            	encoding = HConstants.UTF8_ENCODING;
            
            if (!cline.hasOption("ht") || !cline.hasOption("hc") ||!cline.hasOption("mt") || !cline.hasOption("md")
            		|| !cline.hasOption("mf") || cline.hasOption("help"))
            {
            
            	LOG.info("param error");
            	help.printHelp(CMDLINE, options);
            	return -1;
            }
            
            mysqldb = cline.getOptionValue("md");
            htable = cline.getOptionValue("ht"); 
            mtable = cline.getOptionValue("mt");
            hcolums = cline.getOptionValue("hc");
            mfields  = cline.getOptionValue("mf").split(",");
            mysqlurl = String.format("jdbc:mysql://%s/%s",mysqlhost,mysqldb);
            
            
           
        } catch (Exception e) {
            System.out.println(e);
            e.printStackTrace();
            help.printHelp(CMDLINE, options);
            return -1;
        }
        return 0;
	}
	public static class DBRecord implements Writable, DBWritable {
		String[] dbfields;

		public DBRecord(int fcount,String mykey, String myvalue) {
			this.dbfields = new String[fcount];
			String[] values = myvalue.split(",");
			dbfields[0] = mykey;
			int i = 0;
			for(i = 0; i < values.length; i++)
			{
				dbfields[i] = values[i];
			}
			
			LOG.info("DBRecord fields:"+ Arrays.toString(dbfields));

		}

		public void readFields(DataInput in) throws IOException {
			for (String fld : this.dbfields) {
				fld = Text.readString(in);
			}

		}

		public void write(DataOutput out) throws IOException {
			for (String fld : this.dbfields) {
				Text.writeString(out, fld);
			}

		}

		public void readFields(ResultSet result) throws SQLException {
			int i = 0;
			for (String fld : this.dbfields) {
				i++;
				fld = result.getString(i);

			}

		}

		public void write(PreparedStatement stmt) throws SQLException {

			int i = 0;
			for (String fld : this.dbfields) {
				i++;
				stmt.setString(i, fld);

			}

		}

	}
	

	public static class MyMapper extends MapReduceBase implements Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, ImmutableBytesWritable> {
		@Override
		public void map(ImmutableBytesWritable key,	Result rs,
				OutputCollector<ImmutableBytesWritable, ImmutableBytesWritable> output, Reporter report) throws IOException
		{
		
			String[] thecolums = hcolums.split(" ");
			String[] values = new String[mfields.length];

			String rowkey = new String(key.get());
			LOG.info("rowkey:"+rowkey);
			
			
			int i=0;
			for(i = 0; i < mfields.length; i++)
			{
				try
				{
					values[i] = new String(rs.getValue(thecolums[i].split(":")[0].getBytes(), thecolums[i].split(":")[1].getBytes()),encoding);
				}
				catch(NullPointerException e)
				{
					LOG.info("rowkey:"+rowkey+" column:"+thecolums[i]+" value is null");
					values[i]="";
				}
				catch(Exception e)
				{
					e.printStackTrace();
					values[i]="";
				}
				LOG.info("value :"+ values[i]);
			}
			String value="";
			for(i=0; i< mfields.length; i++)
			{
				value += values[i]+",";
				
			}
			
			output.collect(new ImmutableBytesWritable(values[0].getBytes()),new ImmutableBytesWritable((value).getBytes()));
			
			
		}

		@Override
		public void configure(JobConf job) {
			super.configure(job);
		}
		
	}
	public static class MyReducer extends MapReduceBase implements Reducer<ImmutableBytesWritable, ImmutableBytesWritable,DBRecord, Text>{
		
		@Override
		public void configure(JobConf job) {
			
			super.configure(job);
		}

		@Override
		public void reduce(ImmutableBytesWritable key,
				Iterator<ImmutableBytesWritable> it,
				OutputCollector<DBRecord, Text> output, Reporter report)
				throws IOException {
			String mykey =new String(key.get());
			String myvalue = new String(it.next().get());
			DBRecord record = new DBRecord(mfields.length,mykey,myvalue);
			
			
			
			output.collect(record, new Text());
		}
		
	}


	public static void main(String[] args) throws Exception {
		
		

		conf = HBaseConfiguration.create();
		LOG.info("main args:"+ Arrays.toString(args));
		if(parseArgs(conf,args) < 0) return ;
		
		
		int ret = ToolRunner.run(conf,new CopyHBaseToMysql(args), args);
		System.exit(ret);

	}
	
	@Override
	public int run( String[] args) throws Exception {
		LOG.info("run args:"+ Arrays.toString(args));
				
		createConnection(driverClassName, mysqlurl);

		JobControl control = new JobControl("mysql");
		JobConf job = new JobConf(conf,CopyHBaseToMysql.class);
		
		job.setJarByClass(CopyHBaseToMysql.class);
				
		job.set("mapred.input.dir", htable);
		
		job.set("hbase.mapred.tablecolumns", hcolums);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(ImmutableBytesWritable.class);
		job.setInputFormat(TableInputFormat.class);
		DBConfiguration.configureDB(job, driverClassName, mysqlurl, username, passwd);
		
		DBOutputFormat.setOutput(job, mtable, mfields);
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
	
		
		LOG.info("Job finished!");
		return 0;
	}

	
	



	//connect
	private void createConnection(String driverClassName, String url) throws Exception {
		    Class.forName(driverClassName);
		    connection = DriverManager.getConnection(url,username,passwd);
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


