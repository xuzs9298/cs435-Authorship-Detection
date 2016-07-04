import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AuthorCosineSimilarityAggregatorReducer extends Reducer<Text,Text,Text,Text> {
	
  	private double cosineSimilarity = 0;
  	
    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {	
    	cosineSimilarity = 0.;
    	
    	for (Text val:values){
    		
    		Double currentSimilarityComponent = Double.parseDouble(val.toString());
    		
    		cosineSimilarity+=currentSimilarityComponent;
    		
    	}
    	
    	context.write(key, new Text("@"+Double.toString(cosineSimilarity)));
    }
}
