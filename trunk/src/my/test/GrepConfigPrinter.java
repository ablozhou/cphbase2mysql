package my.test;
import java.util.Map.Entry;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.*;
import org.apache.log4j.Logger;

//http://abloz.com
//date:2012.7.30
// ConfigPrinter:print all settings and properties of hadoop,grep a needed config item
public class GrepConfigPrinter extends Configured implements Tool {
  /*
  static {
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
    Configuration.addDefaultResource("mapred-default.xml");
    Configuration.addDefaultResource("mapred-site.xml");
    //Configuration.addDefaultResource("test.xml");
  }
*/
    private String name = "";
    private String grep = "";
	private final Logger LOG = Logger.getLogger(GrepConfigPrinter.class);
    private final String CMDLINE = "GrepConfigPrinter [-n xmlname] [-g grep_config]";

    
	private int parseArgs(Configuration conf,String[] args)
	{
		HelpFormatter help = new HelpFormatter();
        Options options = new Options();
        options.addOption("h", "help", false, "print program usage");
        options.addOption("n", "xmlname", true, "add xml file name to parse");
        options.addOption("g", "grep_config", true, "grep config item");
        CommandLineParser parser = new BasicParser();
        CommandLine cline;
        try {
            cline = parser.parse(options, args);
            args = cline.getArgs();
//            if (args.length < 1) {
//                help.printHelp(CMDLINE, options);
//                return -1;
//            }
        } catch (ParseException e) {
            System.out.println(e);
            e.printStackTrace();
            help.printHelp(CMDLINE, options);
            return -1;
        }
 

        try {
            if (cline.hasOption('n'))
                name = cline.getOptionValue('n');
            else
                name = "";
            if (cline.hasOption('g'))
                grep = cline.getOptionValue('g');
            else
                grep = "";
            if (cline.hasOption('h'))
            {
            	help.printHelp(CMDLINE, options);
            	return -1;
            }
            
           
        } catch (Exception e) {
            System.out.println(e);
            e.printStackTrace();
            help.printHelp(CMDLINE, options);
            return -1;
        }
        return 0;
	}
  @Override
  public int run(String[] args) throws Exception {
	Configuration conf = getConf();
	
	if(parseArgs(conf,args) < 0) return -1;
	
    if(! name.isEmpty())
    {
    	conf.addResource(name);
    }
	//String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	
	Job job = new Job(conf, "print args");
	job.setJarByClass(GrepConfigPrinter.class);
    
	//System.setProperty("test1", "mytest");
	if(grep.length() > 0 && !conf.get(grep).isEmpty())
	{
		System.out.printf("%s=%s\n", grep, conf.get(grep));
	}
	else
	{
		for (Entry<String, String> entry: conf) {
			
			
	      System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
	    }
	}
    
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new GrepConfigPrinter(), args);
    System.exit(exitCode);
  }
}
