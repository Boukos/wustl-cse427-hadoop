package solution;

//cc SequenceFileWriteDemo Writing a SequenceFile
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

//vv SequenceFileWriteDemo
public class SequenceFileWriter {

	private static String[] keyvaluepair;

	public static void main(String[] args) throws IOException {
		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);
	
		Text key = new Text();
		Text value = new Text();
		SequenceFile.Writer writer = null;
		
		try {
			writer = SequenceFile.createWriter(fs, conf, path,
					key.getClass(), value.getClass());
	   
			File logs = new File("testlog.txt");
			try {
				FileReader fileReader = new FileReader(logs);
				BufferedReader bufferedReader = new BufferedReader(fileReader);
				String line;
				while ((line = bufferedReader.readLine()) != null) {
					// process the line.
					//if (!(line.charAt(0) == ";")){
					//	DATA+=line;
					//}
					keyvaluepair = line.split(" - - ");
					key.set(keyvaluepair[0]);
					value.set(keyvaluepair[1]);
					writer.append(key, value);
				}
				bufferedReader.close();
				fileReader.close();
			}	catch (IOException e) {
				System.out.println(e);
			}

			/*for (int i = 0; i < 100; i++) {
				key.set(100 - i);
				value.set(DATA[i % DATA.length]);
				System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
				writer.append(key, value);
			}*/
		} finally {
			 IOUtils.closeStream(writer);
		}
	}
}
//^^ SequenceFileWriteDemo