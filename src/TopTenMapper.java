import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopTenMapper extends Mapper<Object, Text, Text, Text>{

    private final static IntWritable one = new IntWritable(1);
  	
  	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {	
  		
  			String newLine = value.toString();
  			
  			String[] result = newLine.split("@");	
  			
  			String author = result[0];
  			String cosineSimilarityComponentStr = result[1];
  			
  			context.write(new Text(author), new Text(cosineSimilarityComponentStr));
  		} 
  		
  	}
}
