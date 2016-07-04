import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MysteriousDocTermFrequencyMapper extends Mapper<Object,Text, Text, Text>{
  	private Scanner scanner;
  	
	private Integer maxCount;
	private String records;
	private String finalRecords;
	private String author;
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {	
		maxCount = -1;
		records = "";
  		author = "";
  		finalRecords="";
  		
  		String currentLine = value.toString();
  			
  			if (currentLine.split("@").length==2)
  			{	
  				String authorUnigram = currentLine.split("@")[0]; 
  				
  				if(authorUnigram.split("\\$").length==2){
  	  				
  					Integer currentCount = Integer.parseInt(currentLine.split("@")[1]);
  					
  					author = authorUnigram.split("\\$")[0].trim();
  					String currentUnigram = authorUnigram.split("\\$")[1];
  					
  					context.write(new Text(author), new Text(currentUnigram+"@"+Integer.toString(currentCount)));
  					
  				}else{
  					
  				}
  			}else{
  			}  		
  		
  	}

}