package stubs;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.junit.Before;
import org.junit.Test;

public class SentimentPartitionTest {

	//SentimentPartitioner<Text, IntWritable> mpart;
	SentimentPartitioner mpart;
	
	@Test
	public void testSentimentPartition() {

		//spart=new SentimentPartitioner<Text, IntWritable>();
		SentimentPartitioner spart = new SentimentPartitioner();
		spart.setConf(new Configuration());
		//int result;		
		
		/*
		 * Test the words "love", "deadly", and "zodiac". 
		 * The expected outcomes should be 0, 1, and 2. 
		 */
        
 		/*
		 * TODO implement
		 */
		int result1 = spart.getPartition(new Text("love"), new IntWritable(1), 3);
		int result2 = spart.getPartition(new Text("deadly"), new IntWritable(1), 3);
		int result3 = spart.getPartition(new Text("zodiac"), new IntWritable(1), 3);
	    System.out.println(result1);
	    System.out.println(result2);
	    System.out.println(result3);
		assertEquals(result1, 0);
		assertEquals(result2, 1);
		assertEquals(result3, 2);
	}

}
