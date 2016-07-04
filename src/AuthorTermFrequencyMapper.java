import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AuthorTermFrequencyMapper extends Mapper<Object,Text, Text, Text>{
//  	private Scanner scanner;
  	
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
  			
//  		   System.out.println(currentLine +"----------Current split length"+currentLine.split("@").length);
  		   
  			if (currentLine.split("@").length==2)
  			{	
  				String authorUnigram = currentLine.split("@")[0]; 
  				
//  				System.out.println("AuthorUnigram = "+authorUnigram+"-----authorUnigram Split length:  "+authorUnigram.split("\\$").length);
  				
  				if(authorUnigram.split("\\$").length==2){
  	  				
  					Integer currentCount = Integer.parseInt(currentLine.split("@")[1]);
  	  									
  					author = authorUnigram.split("\\$")[0].trim();
  					String currentUnigram = authorUnigram.split("\\$")[1];
  					
  					context.write(new Text(author), new Text(currentUnigram+"@"+Integer.toString(currentCount)));
  					
  				}else{
  					/**Bad record*/
  				}
  			}else{
  				/** Bad line, unable to get a count
  				  */
  			}  		
  		
  	}

}