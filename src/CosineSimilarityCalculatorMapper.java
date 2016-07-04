import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CosineSimilarityCalculatorMapper extends Mapper<Object, Text, Text, Text>{
	
  	private Scanner scanner;
  	
  	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {	
  		
  			String newLine = value.toString();
  			String[] splitResult = newLine.split("@");
  			
  			String currentUnigram = splitResult[1];
  			String author = splitResult[0].trim();
  			String TFIDFStr  = splitResult[2];
  			String associatedLengthStr = splitResult[3];
  			
  			context.write(new Text(currentUnigram), new Text(author+"@"+TFIDFStr+"@"+associatedLengthStr));
  			
  	} 

}
