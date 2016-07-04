import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UnigramByHowManyAuthorsReducer extends Reducer<Text,Text,Text,Text> {
	
  	private int unigramByHowManyAuthors = 0;
  	private ArrayList<String> records;
  	
    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {	
    	unigramByHowManyAuthors=0;
    	records = new ArrayList<String>();
    	
    	for (Text val:values){
    		unigramByHowManyAuthors++;
    		records.add(val.toString());
    	}
    	
    	for (int i=0;i<records.size();i++){
    		String currentRecord=records.get(i);
 
    		Integer totalAuthors = Integer.parseInt(currentRecord.split("\\$")[1]);
    		Double currentTF = Double.parseDouble(currentRecord.split("\\$")[2]);
    		String author = currentRecord.split("\\$")[0];
    		
    		Double currentIDF = Math.log(((double)totalAuthors)/((double)unigramByHowManyAuthors))/Math.log(2);
    		String currentTFIDFStr = Double.toString(currentTF*currentIDF);
    			
    		String currentValue = "@"+key.toString()+"@"+currentRecord.split("\\$")[1]+"@"+currentIDF+"@"+currentTFIDFStr;
    		//value format: unigram \t totalAuthors \t currentIDF \t currentTFIDF
    		
    		//output  format:    author \t  unigram \t totalAuthors \t currentIDF \t currentTFIDF
    		context.write(new Text(author), new Text(currentValue));
    	}
    	
    }
}