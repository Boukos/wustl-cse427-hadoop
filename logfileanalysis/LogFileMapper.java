package stubs;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Example input line:
 * 96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] "GET /cat.jpg HTTP/1.1" 200 12433 \n
 *
 */

public class LogFileMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    /*
     * TODO implement
     */
	  String line = value.toString();
	  String ip = "";
	  for (String entry : line.split("\n")) {
		  //ip = (entry.split(" -", 2)[0]).substring(7);
		  ip = (entry.split(" -", 2)[0]);
	      if (ip.length() > 0) {
	        context.write(new Text(ip), new IntWritable(1));
	      }
	  }
	  
  }
}