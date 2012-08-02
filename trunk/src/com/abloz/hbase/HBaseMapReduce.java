package com.abloz.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;

import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

//import cn.jj.hbase.BuildScoreToplist.buildTmpScoreToplistMapper;
//import cn.jj.hbase.BuildScoreToplist.buildTmpScoreToplistReducer;

public class HBaseMapReduce {

	public static Configuration conf =  HBaseConfiguration.create();
	
	
	public static class buildScoreMapper extends 
	TableMapper<ImmutableBytesWritable,  ImmutableBytesWritable> 
	{
		//map,get english score over 80 
		@Override
		public  void map(ImmutableBytesWritable row,
				Result value, Context context) throws IOException
		{
			
			try
			{
				String srow = new String(row.get());
				System.out.println("row:"+srow);
				ImmutableBytesWritable rowkey= new ImmutableBytesWritable(Bytes.toBytes(srow));
				String family="class";
				String qualifier="english";
				String v = new String(value.getValue(family.getBytes(), qualifier.getBytes()));
				ImmutableBytesWritable rowValue= new ImmutableBytesWritable(v.getBytes());
				
				context.write(rowkey, rowValue);
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
		
	}
	
	public static class buildScoreReduce extends
	TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable>
	{
		@Override
		public void reduce(ImmutableBytesWritable key,
				Iterable<ImmutableBytesWritable> values, Context context)
						throws IOException, InterruptedException 
						
		{
			
			try
			{
				String rowKey=Bytes.toString(key.get());
				Put put = new Put(rowKey.getBytes());
				String fm = "info";
				String vs="";
				System.out.println("rowkey:"+rowKey);
				for(ImmutableBytesWritable value:values)
				{
					String v=Bytes.toString(value.get());
					vs += " "+v;
					
				}
				System.out.println("values:"+vs);
				String tk="";
				put.add(fm.getBytes(),tk.getBytes(),Bytes.toBytes(vs));
				context.write(key, put);
				
				
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
	}
	public static void main(String[] args)
	{
		//Configuration conf = new HBaseConfiguration.create();
		try
		{
			Job job = new Job(conf,"test map reduce of htable");
			job.setJarByClass(HBaseMapReduce.class);
			//job.setMapperClass(buildScoreMapper.class);
			//job.setReducerClass(null);
			//job.setInputFormatClass(TextInputFormat.class);
	        //job.setMapOutputKeyClass(Text.class);
	        //job.setMapOutputValueClass(Text.class);
	        //FileInputFormat.addInputPath(job, new Path("test1.txt"));
	        //FileOutputFormat.setOutputPath(job, new Path("output"));
	        
	        Scan scan = new Scan();
			scan.setCaching(500);
			// scan.setTimeRange(begintime, endtime);
			//Filter filter = new PrefixFilter(Bytes.toBytes(conf.get("date")));
			//scan.setFilter(filter);
			scan.setCacheBlocks(false); // don't set to true for MR jobs
			TableMapReduceUtil.initTableMapperJob("student", scan,
					buildScoreMapper.class, ImmutableBytesWritable.class,
					ImmutableBytesWritable.class, job);
			TableMapReduceUtil.initTableReducerJob("t1", buildScoreReduce.class, job);
			//job.setNumReduceTasks(1);
			boolean ret = job.waitForCompletion(true);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
	}
}
