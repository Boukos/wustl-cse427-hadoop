package stubs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


/* 
 * To define a map function for your MapReduce job, subclass 
 * the Mapper class and override the map method.
 * The class definition requires four parameters: 
 *   The data type of the input key
 *   The data type of the input value
 *   The data type of the output key (which is the input key type 
 *   for the reducer)
 *   The data type of the output value (which is the input value 
 *   type for the reducer)
 */

public class LetterMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	
	private static final Logger LOGGER = Logger.getLogger (LetterMapper.class.getName());
	private int csflag = 0;
	
  /*
   * The map method runs once for each line of text in the input file.
   * The method receives a key of type LongWritable, a value of type
   * Text, and a Context object.
   */
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    /*
     * Convert the line, which is received as a Text object,
     * to a String object.
     */
    String line = value.toString();
	Text initial = new Text();
	DoubleWritable wordLength = new DoubleWritable();
	
	/*
     * The line.split("\\W+") call uses regular expressions to split the
     * line up by non-word characters.
     * 
     * If you are not familiar with the use of regular expressions in
     * Java code, search the web for "Java Regex Tutorial." 
     */
	if(csflag == 1){
	    for (String word : line.split("\\W+")) {
	        if (word.length() > 0) {
	        	initial.set(String.valueOf(word.charAt(0)));
	        	wordLength.set(word.length());
	        	/*
	        	 * Call the write method on the Context object to emit a key
	        	 * and a value from the map method.
	        	 */
	        	context.write(initial, wordLength);
	        }
	      }
	}
	else{
	    for (String word : line.split("\\W+")) {
	        if (word.length() > 0) {
	        	initial.set(String.valueOf(word.charAt(0)).toLowerCase());
	        	wordLength.set(word.length());
	        	context.write(initial, wordLength);
	        }
	    }
	}
  }
  
  public void setup(Context context){
	  Configuration conf = context.getConfiguration();
	  csflag = conf.getInt("caseSensitive",1);

	  LOGGER.info("Logger working");
	  if (LOGGER.isDebugEnabled()){
			LOGGER.debug(csflag);
	  }
  }
  
  public void cleanup(Context context) throws IOException, InterruptedException{

  }
  
}
