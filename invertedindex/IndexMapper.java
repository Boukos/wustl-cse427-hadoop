package stubs;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.util.StringTokenizer;

public class IndexMapper extends Mapper<Text, Text, Text, Text> {
	
	private boolean caseSensitive = true;
    private Text word =new Text();
    private Text filename= new Text();
	
  @Override
  public void map(Text key, Text value, Context context) throws IOException,
      InterruptedException {

    /*
     * TODO implement	
     */
	  String filenameStr = ((FileSplit) context.getInputSplit()).getPath().getName();
	  filename = new Text(filenameStr);
	  String line = value.toString();
	  
	  if (!caseSensitive) {
		  line = line.toLowerCase();
	  }

	  StringTokenizer tokenizer = new StringTokenizer(line);
	  while (tokenizer.hasMoreTokens()) {
		  word.set(tokenizer.nextToken());			
		  context.write(word,new Text(filename+"@"+key) );
	  }
  }
}