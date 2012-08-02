package my.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

public class TestConfigure {

	public TestConfigure() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    conf.addResource("test.xml");
	    System.out.println(conf.get("fs.default.name"));
	    System.setProperty("test1", "t1");
	    System.out.println(conf.get("test1"));
	    //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    System.out.println(conf.get("test2"));

	}

}
