package stubs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

public class SentimentPartitioner extends Partitioner<Text, IntWritable> implements
    Configurable {

  private Configuration configuration;
  Set<String> positive = new HashSet<String>();
  Set<String> negative = new HashSet<String>();

  /**
   * Set up the positive and negative hash set in the setConf method.
   */
  @Override
  public void setConf(Configuration configuration) {
     /*
     * Add the positive and negative words to the respective sets using the files 
     * positive-words.txt and negative-words.txt.
     */
    /*
     * TODO implement 
     */
	  this.configuration = configuration;
	  
	  File pwords = new File("positive-words.txt");
	  File nwords = new File("negative-words.txt");
	  
	  try {
		  FileReader fileReader = new FileReader(pwords);
		  BufferedReader bufferedReader = new BufferedReader(fileReader);
		  String line;
		  while ((line = bufferedReader.readLine()) != null) {
			  // process the line.
			  if (!(line.charAt(0) == ';')){
				  positive.add(line);
			  }
		  }
		  bufferedReader.close();
		  fileReader.close();
	  }	catch (IOException e) {
	      System.out.println(e);
	  }
	  
	  try {
		  FileReader fileReader = new FileReader(nwords);
		  BufferedReader bufferedReader = new BufferedReader(fileReader);
		  String line;
		  while ((line = bufferedReader.readLine()) != null) {
			  // process the line.
			  if (!(line.charAt(0) == ';')){
				  negative.add(line);
			  }
		  }
		  bufferedReader.close();
		  fileReader.close();
	  }	catch (IOException e) {
	      System.out.println(e);
	  }

	  System.out.println("positive words "+positive.contains("happy"));
	  System.out.println("negative words "+negative.contains("sad"));
	  /*
	  // positive words:
	  try {
		  List<String> lines = Files.readAllLines(Paths.get("positive-words.txt"),
				  Charset.defaultCharset()) ;
		  for (String line : lines) {
			  if (!(line.charAt(0) == ';')){
				  positive.add(line);
			  }
			  // System.out.println(line\
		  }
	  } catch (IOException e) {
	      System.out.println(e);
	  }
	  
	  // negative words:
	  try {
		  List<String> lines = Files.readAllLines(Paths.get("negative-words.txt"),
				  Charset.defaultCharset()) ;
		  for (String line : lines) {
			  if (!(line.charAt(0) == ';')){
				  negative.add(line);
			  }
			  // System.out.println(line\
		  }
	  } catch (IOException e) {
	      System.out.println(e);
	  }*/
  }

  /**
   * Implement the getConf method for the Configurable interface.
   */
  @Override
  public Configuration getConf() {
    return configuration;
  }

  /**
   * You must implement the getPartition method for a partitioner class.
   * This method receives the words as keys (i.e., the output key from the mapper.)
   * It should return an integer representation of the sentiment category
   * (positive, negative, neutral).
   * 
   * For this partitioner to work, the job configuration must have been
   * set so that there are exactly 3 reducers.
   */
  public int getPartition(Text key, IntWritable value, int numReduceTasks) {
    /*
     * TODO implement
     * Change the return 0 statement below to return the number of the sentiment 
     * category; use 0 for positive words, 1 for negative words, and 2 for neutral words. 
     * Use the sets of positive and negative words to find out the sentiment.
     *
     * Hint: use positive.contains(key.toString()) and negative.contains(key.toString())
     * If a word appears in both lists assume it is positive. That is, once you found 
     * that a word is in the positive list you do not need to check if it is in the 
     * negative list. 
     */
	  
	  if (positive.contains(key.toString())){
		  return 0;
	  }
	  else if (negative.contains(key.toString())){
		  return 1;
	  }
	  else{
		  return 2;
	  }
	  
  }
}
