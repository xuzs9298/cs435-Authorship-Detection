import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopTenCombiner extends Reducer<Text,Text,Text,Text> {
	
  	private double totalCosineSimilarity;
  	
    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {	
    	totalCosineSimilarity=0;
    	
    	for (Text val:values){
    		double currentCosineSimilarity = Double.parseDouble(val.toString());
    		totalCosineSimilarity+=currentCosineSimilarity;
    	}
    	
    	String cosineValueStr = Double.toString(totalCosineSimilarity);
    	String finalValueStr = key.toString()+"@"+cosineValueStr;
    	
    	context.write(new Text("key"), new Text(finalValueStr));
    }
}
