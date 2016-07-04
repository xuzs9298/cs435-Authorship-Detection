import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AuthorCountMapper extends Mapper<Object,Text, Text, Text>{
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {	
		
  			String currentLine = value.toString();
  			currentLine = currentLine.trim();
  			
  			context.write(new Text("Key"), new Text(currentLine));
  	}

}