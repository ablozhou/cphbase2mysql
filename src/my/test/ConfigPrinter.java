package my.test;
import java.util.Map.Entry;
import org.apache.hadoop.conf.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.*;

// ConfigPrinter:print all settings and properties of hadoop
public class ConfigPrinter extends Configured implements Tool {
  /*
  static {
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
    Configuration.addDefaultResource("mapred-default.xml");
    Configuration.addDefaultResource("mapred-site.xml");
    //Configuration.addDefaultResource("test.xml");
  }
*/
  @Override
  public int run(String[] args) throws Exception {
	Configuration conf = getConf();
	
    
	conf.addResource("test2.xml");
	
	
	Job job = new Job(conf, "print args");
	job.setJarByClass(ConfigPrinter.class);
    
	System.setProperty("test1", "mytest");
	for (Entry<String, String> entry: conf) {
      System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
    }
    
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new ConfigPrinter(), args);
    System.exit(exitCode);
  }
}
