package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
  private String prevword = "";
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    /*
     * TODO implement
     */
	  String line = value.toString();
	  for (String word : line.split("\\W+")) {
		  if (word.length() > 0) {
			  context.write(new Text(prevword+" "+word), new IntWritable(1));
			  prevword = word;
		  }
	  }
  }
}
