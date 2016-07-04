import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AttributesLengthReducer extends Reducer<Text,Text,Text,Text> {
	  
  	private Double lengthOFAttributes;
  	private ArrayList<String> lines;
  	
    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
    	lengthOFAttributes=0.;
    	lines = new ArrayList<String>();
    	
    	for (Text val: values){
    		Double currentTFIDF = Double.parseDouble(val.toString().split("@")[1]);
    		
    		lengthOFAttributes+=currentTFIDF*currentTFIDF;
    		
    		lines.add(val.toString());
    	}
    	
    	lengthOFAttributes = Math.sqrt(lengthOFAttributes);
    	
    	for(int i=0;i<lines.size();i++){
    		String currentLine = lines.get(i);
    		
    		String currentLengthStr = Double.toString(lengthOFAttributes);
    		String currentUnigram = currentLine.split("@")[0];
    		String currentTFIDFStr = currentLine.split("@")[1];
    		
    		String currentValue = "@"+currentUnigram+"@"+currentTFIDFStr+"@"+currentLengthStr;
    
    		context.write(key, new Text(currentValue));
    	}
    }
}
