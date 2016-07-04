import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends Mapper<Object, Text, Text, Text>{
  	
  	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {	
			String newLine = value.toString();
			
			String[] result = newLine.split("<===>");
			String author = result[0];
			String lineBody = result[1];
			
			String[] words = lineBody.replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+");
			
			for (int j=0;j<words.length;j++){
				String authorUnigramKey = author+"@"+words[j];
				
				context.write(new Text(authorUnigramKey), new Text(""));
			}
  	} 

}
