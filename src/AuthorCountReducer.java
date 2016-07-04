import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AuthorCountReducer extends Reducer<Text,Text,Text,Text> {
	  
  	private Integer totalAuthors;
  	private ArrayList<String> lines;
  	
    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
    	totalAuthors=0;
    	lines = new ArrayList<String>();
    	
    	for (Text val: values){
    		totalAuthors++;
    		lines.add(val.toString());
    	}
    	
    	for(int i=0;i<lines.size();i++){
    		String currentLine = lines.get(i);
    		
    		String[] splitResult = currentLine.split(";");
    		
    		if(splitResult.length>=2){
    		
    			String author = splitResult[0];
    			
    			String tfs = splitResult[1];
 
    			String finalValue=author+"<--->"+Integer.toString(totalAuthors)+";"+tfs;
    		
    			context.write(new Text(""), new Text(finalValue));
    			//format ""\tauthor:totalAuthor;unigram:tf1,unigram:tf2,...
    		}
    	}
    }
}
