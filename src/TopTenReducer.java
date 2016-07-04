import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopTenReducer extends Reducer<Text,Text,Text,Text> {
	
  	private TreeMap<Double,String> result;
  	private String currentAuthor;
  	private Double currentCosineSimilarityValue;
  	
    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {	
    	
    	result = new TreeMap<Double, String>(Collections.reverseOrder());
    	currentAuthor="";
    	currentCosineSimilarityValue=0.;
    	
    	for (Text val:values){
    		currentAuthor = val.toString().split("@")[0];
    		currentCosineSimilarityValue = Double.parseDouble(val.toString().split("@")[1]);
    		
    		if(result.size()<10){
    			result.put(new Double(currentCosineSimilarityValue), currentAuthor);
    		}else{
    			if (currentCosineSimilarityValue>result.lastKey()){
    				result.remove(result.lastKey());
    				result.put(currentCosineSimilarityValue, currentAuthor);
    			}else{
    				/**
    				 * current value smaller than last one, whihc is the smallest in current Top ten do nothing
    				 * */
    			}
    		}
    	}
    	
    	
    	
    	for (Map.Entry<Double, String> entry: result.entrySet()){
    		
    		String cosineSimilarityStr = Double.toString(entry.getKey());
    		
    		context.write(new Text(entry.getValue()), new Text(cosineSimilarityStr));
    	}
    }
}
