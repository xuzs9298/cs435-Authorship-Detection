import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CosineSimilarityCalHelperReducer extends Reducer<Text,Text,Text,Text> {
	
	private double globalIDF;
  	private double currentDFForCurrentKey;
	private int hasMysteriousWordFlag;
  	private int firstTimeFlag;
  	private int totalValuesCount;
  	private Double TFIDFForMysteriousWord;
  	private int totalAuthors;
	
    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {	
    	globalIDF=0;
    	currentDFForCurrentKey=0;
    	hasMysteriousWordFlag=0;
    	firstTimeFlag=1;
    	totalValuesCount=0;
    	TFIDFForMysteriousWord=0.;
    	totalAuthors=0;
    	
    	/**
    	 * First find the record for the mysterious document
    	 * */
    	 for (Text val: values){
    		 
    		 String author = val.toString().split("@")[0];
    		 
    		 if(author.equals("$$$DummyAuthor$$$")){
    			 /**
    			  * This is a record from the mysterious Document
    			  * */
    			 currentDFForCurrentKey = Double.parseDouble(val.toString().split("@")[1]);
    			 
    			 totalAuthors = Integer.parseInt(val.toString().split("@")[2]);
    			 
    			 hasMysteriousWordFlag=1;
    		 }else{
    			 
        		 /**
        		  * Get the global IDF
        		  * */
        		 if (firstTimeFlag==1) {
    				firstTimeFlag=0;
    				globalIDF=Double.parseDouble(val.toString().split("@")[2]);
    				
        		 }
        		/**
        		 * Write current record to output file
        		 * */ 
        		 String currentUnigram = key.toString();
        		 String currentAuthor = val.toString().split("@")[0];
        		 String currentTFIDFStr = val.toString().split("@")[3];
        		
        		 String currentValue = "@"+currentUnigram+"@"+currentTFIDFStr;
        		 
//        		 System.out.println("currentAuthor:"+currentAuthor+"---"+currentValue);
        		 
        		 context.write(new Text(currentAuthor),new Text(currentValue));
        		 
    		 }
    		 
    		 totalValuesCount++;
    	 }
    	 
    	 
    	/**
    	 *  
    	 * if exist:
    	 * 		start iterate through the rest and calculate the 
    	 * 		Cosine similarity component for each author and 
    	 * 		write(author, CosineSimilarityComponent
    	 * */
    	 if(hasMysteriousWordFlag==1){
    		 
    		 if(totalValuesCount>=2){
    			 /**
    			  * This unigram from the mysterious doc also appear in the base document
    			  *  use the Gobal IDF from the base Doc to calculate the TF*IDF for the 
    			  *  unigram for the mysterious author, and write
    			  *  
    			  *  Write  key($$$DummyAuthor$$$) value(unigram \t TFIDF(global IDF))
    			  * */
    			 TFIDFForMysteriousWord = globalIDF*currentDFForCurrentKey;
    			 String currentValue = "@"+key.toString()+"@"+Double.toString(TFIDFForMysteriousWord);

//        		 System.out.println("currentAuthor:"+"$$$DummyAuthor$$$"+"---"+currentValue+"     log(N/n)");
        		 
        		 context.write(new Text("$$$DummyAuthor$$$"), new Text(currentValue));

    		 }else{
    			 /**
    			  * current unigram only appear in the mysterious text, there is no need to 
    			  * calculate the TFIDF for it becuase it wouldn't be used finally 
    			  * when calculating the cosineSimilarity
    			  * 
    			  * But it will be needed for calculate the length of the vector
    			  * 
    			  * IDF of it = log2(N/1)
    			  * */
    			 double currentTotalAuthor = totalAuthors;
    			 
    			 TFIDFForMysteriousWord = currentDFForCurrentKey*Math.log(currentTotalAuthor)/Math.log(2.0);
    			 
    			 /**
    			  * Write  key($$$DummyAuthor$$$) value(unigram \t TFIDF(log2(N/1)))
    			  * */
    			 String currentValue = "@"+key.toString()+"@"+Double.toString(TFIDFForMysteriousWord);
    			 
//        		 System.out.println("currentAuthor:"+"$$$DummyAuthor$$$"+"---"+currentValue+"   log(N/1)");
    			 
    			 context.write(new Text("$$$DummyAuthor$$$"), new Text(currentValue));
    		 }
    		 
    	 }
    	 /**
    	 * else:
    	 * 		do nothing, which means there is not such word
    	 * 		in the mysterious document,which means there is no
    	 * 		need to calculate it.
    	 * 
    	 * 		those words in the base attributes file already have their TFIDF
    	 * 		and the record has already been written to outpur file
    	  * */
    	 else{
    	 }
    	 
    }
}
