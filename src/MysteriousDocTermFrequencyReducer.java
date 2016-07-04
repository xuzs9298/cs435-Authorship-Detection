import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MysteriousDocTermFrequencyReducer extends Reducer<Text,Text,Text,Text> {
  	
	private String tfs;
	private ArrayList<String> records;
	private Integer maxUnigramCount;
	
    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
    	tfs="";   
    	maxUnigramCount = -1;
    	records = new ArrayList<String>();
    	
    	for (Text val: values){
    		records.add(val.toString());
    		
    		int currentCount = Integer.parseInt(val.toString().split("@")[1]);
    		
    		if(currentCount>maxUnigramCount){
    			maxUnigramCount = currentCount;
    		}    		
    	}

    	
    	for (int i=0;i<records.size();i++){
    		String currentRecord=records.get(i);
    		
    		if(currentRecord.split("@").length==2)
    		{
    			String currentUnigram = currentRecord.split("@")[0];
    			Integer currentWordCount = Integer.parseInt(currentRecord.split("@")[1]);
    			
    			double currentTF = ((double)currentWordCount)/((double)maxUnigramCount);
    			
    			String gramTFUnitValue = currentUnigram+":"+currentTF;
    			
    			tfs+=gramTFUnitValue+"@";
    			
    		}else{
    		}
    	}
    	
    	if(tfs.length()>=2){
    		tfs = tfs.substring(0, tfs.length()-1); // remove last separator '@'
    		
    		String[] tfsArray = tfs.split("@");
    		
    		for (int i=0;i<tfsArray.length;i++){
    			String currentUnit = tfsArray[i];
    			
    			if(currentUnit.split(":").length==2){
        			String currentUnigram = currentUnit.split(":")[0];
        			String currentTF = currentUnit.split(":")[1];
        			
        			/**set up dummy authoe name so that this mysterious attributes can 
        			 *be recignized when calculate the cosine similarity 
        			*/
        			context.write(new Text("$$$DummyAuthor$$$"), new Text("@"+currentUnigram+'@'+currentTF));
    			}
    		}
    		
    	}else{
    		
    	}
    }
}
