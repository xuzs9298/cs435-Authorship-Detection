import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AttributesLengthMapper extends Mapper<Object,Text, Text, Text>{
  	
//	private Scanner scanner;
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {	
		
//  		String valueString = new String(value.copyBytes(),"UTF-8");
  		
//  		scanner = new Scanner(valueString);
  		
//  		while(scanner.hasNextLine()){
//  			String currentLine = scanner.nextLine();
  		    String currentLine = value.toString();
  			currentLine = currentLine.trim();
  			
  			String author = currentLine.split("@")[0];
  			
  			String currentValue = currentLine.split("@")[1]+"@"+currentLine.split("@")[2];
  			
  			context.write(new Text(author), new Text(currentValue));
//  		}
  	}

}