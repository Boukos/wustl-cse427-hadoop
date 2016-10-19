package stubs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;

//import com.sun.xml.bind.v2.schemagen.xmlschema.List;

public class TestProcessLogs {

  /*
   * Declare harnesses that let you test a mapper, a reducer, and
   * a mapper and a reducer working together.
   */
  MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
  ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
  MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

  /*
   * Set up the test. This method will be called before every test.
   */
  @Before
  public void setUp() {

    /*
     * Set up the mapper test harness.
     */
    LogFileMapper mapper = new LogFileMapper();
    mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
    mapDriver.setMapper(mapper);

    /*
     * Set up the reducer test harness.
     */
    SumReducer reducer = new SumReducer();
    reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
    reduceDriver.setReducer(reducer);

    /*
     * Set up the mapper/reducer test harness.
     */
    mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>();
    mapReduceDriver.setMapper(mapper);
    mapReduceDriver.setReducer(reducer);
  }

  /*
   * Test the mapper.
   */
  @Test
  public void testMapper() {

    /*
     * TODO: implement
     */
    mapDriver.withInput(new LongWritable(1), new Text("    1   96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] \"GET /cat.jpg HTTP/1.1\" 200 12433 \n"));
    mapDriver.withOutput(new Text("96.7.4.14"), new IntWritable(1));
    try {
    	System.out.println("p1");
    	System.out.println(mapDriver.run());
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    mapDriver.runTest();
  }

  /*
   * Test the reducer.
   */
  @Test
  public void testReducer() {

    /*
     * TODO: implement
     */
	  List<IntWritable> values = new ArrayList<IntWritable>();
	    values.add(new IntWritable(1));
	    values.add(new IntWritable(1));
	    reduceDriver.withInput(new Text("96.7.4.14"), values);
	    reduceDriver.withOutput(new Text("96.7.4.14"), new IntWritable(2));
	    try {
	    	System.out.println("p2");
	    	System.out.println(reduceDriver.run());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    reduceDriver.runTest();
	    
  }


  /*
   * Test the mapper and reducer working together.
   */
  @Test
  public void testMapReduce() {

    /*
     * TODO: implement
     */
	  mapReduceDriver.withInput(new LongWritable(1), new Text("        96.7.4.14"));
	    mapReduceDriver.addOutput(new Text("96.7.4.14"), new IntWritable(1));
	    try {
	    	System.out.println("p3");
	    	System.out.println(mapReduceDriver.run());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    mapReduceDriver.runTest();
	    
  }
}

