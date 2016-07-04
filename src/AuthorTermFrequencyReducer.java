import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AuthorTermFrequencyReducer extends Reducer<Text,Text,Text,Text> {
  	
	private String tfs;
	private ArrayList<String> records;
	private Integer maxUnigramCount;
	
    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
    	tfs="";   
    	maxUnigramCount = -1;
    	records = new ArrayList<String>();
    	
    	for (Text val: values){
    		
    		if(val.toString().split("@").length>=2){
    		
    			records.add(val.toString());
    		
    			int currentCount = Integer.parseInt(val.toString().split("@")[1]);
    		
    			if(currentCount>maxUnigramCount){
    				maxUnigramCount = currentCount;
    			}
    			
    		}else{
    			continue;
    		}
    	}

    	
//    	for (int i=0;i<records.size();i++){
    	for (String str: records){
//    		String currentRecord=records.get(i);
    		String currentRecord = str;
    		
    		if(currentRecord.split("@").length==2)
    		{
    			String currentUnigram = currentRecord.split("@")[0];
    			Integer currentWordCount = Integer.parseInt(currentRecord.split("@")[1]);
    			
    			double currentTF = ((double)currentWordCount)/((double)maxUnigramCount);
    			
    			String gramTFUnitValue = currentUnigram+":"+currentTF;
    			
    			tfs+=gramTFUnitValue+"@";
    			
    		}else{
    			/**
    			 * Bad record
    			 * */
    		}
    	}
    	
    	tfs = tfs.substring(0, tfs.length()-1); // remove last @
    		
    	String valueStr = key.toString()+";"+tfs;
    		//format  author;unigram:tf1@unigram:tf2@ ...
    		
//    	System.out.print("Writing--------"+valueStr);
    		
    	context.write(new Text("  "), new Text(valueStr));
    		//   fakeKey\tauthor;unigram: tf, unigram: tf, ....
    		
    }
}
