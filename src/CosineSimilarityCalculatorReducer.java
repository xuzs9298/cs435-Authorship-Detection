import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CosineSimilarityCalculatorReducer extends Reducer<Text,Text,Text,Text> {
	
  	private int counts = 0;
  	private Double CosineSimilarityComponent;
  	private int isInMysteriousDocFlag;
  	private ArrayList<String> baseDocRecords;
  	private int totalRecordsCount;
  	
  	private Double mysteriousUnigramTFIDF;
  	private Double mysteriousAssociatedLength;
  	
    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {	
    	
    	mysteriousUnigramTFIDF = 0.;
    	mysteriousAssociatedLength = 0.;
    	
    	totalRecordsCount = 0;
    	CosineSimilarityComponent = 0.;
    	isInMysteriousDocFlag = 0;
    	baseDocRecords = new ArrayList<String>();
    	
    	for (Text val: values){
    		String currentStr = val.toString();
    		
    		String currentAuthor = currentStr.split("@")[0];
    		
    		if(currentAuthor.equals("$$$DummyAuthor$$$")){
    			isInMysteriousDocFlag = 1;
    			
    			mysteriousUnigramTFIDF = Double.parseDouble(currentStr.split("@")[1]);
    			mysteriousAssociatedLength = Double.parseDouble(currentStr.split("@")[2]);
    		}else{
    			baseDocRecords.add(currentStr);
    		}
    		
    		totalRecordsCount++;
    	}
    	
    	if(isInMysteriousDocFlag==1){
    	/**
    	 * There exist current unigram in the mysterious Doc.
    	 * */
    		if(baseDocRecords.size()>0){
    		/**
    		 * There also exist current unigram in the base doc
    		 * */
    			
    			/**
    			 * iterate these records and do cosine similarity cal
    			 * and associate the cosine similarity to corresponding author
    			 * */
    			for (int i=0;i<baseDocRecords.size();i++)
    			{
    				String currentRecord = baseDocRecords.get(i);
    				
    				Double currentCosineSimilarityComponent = 0.;
    				
    				Double currentBaseDocTFIDF = Double.parseDouble(currentRecord.split("@")[1]);
    				Double currentBaseDocAssociatedLength = Double.parseDouble(currentRecord.split("@")[2]);
    				String currentBaseDocAuthor = currentRecord.split("@")[0];
    				
    				currentCosineSimilarityComponent = (currentBaseDocTFIDF*mysteriousUnigramTFIDF)/(mysteriousAssociatedLength*currentBaseDocAssociatedLength);
    				
    				String currentCosineSimilarityComponentStr = Double.toString(currentCosineSimilarityComponent);
    				
    				context.write(new Text(currentBaseDocAuthor), new Text("@"+currentCosineSimilarityComponentStr));
    			}
    			
    		}else{
    		
    		
    	}else{
    		
    	}
    	
    }
}
