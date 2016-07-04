import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Scanner;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CosineSimilarityCalHelperMapper extends Mapper<Object, Text, Text, Text>{
	
  	private Scanner scanner;
  	private int totalAuthors;
  	private ArrayList<String> mysteriousRecords;
  	private int firstTimeFlag;
  	
  	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {	
  		
//  		String valueString = new String(value.copyBytes(),"UTF-8");
  		totalAuthors=1710;
  		mysteriousRecords = new ArrayList<String>();
  		firstTimeFlag = 1;
  		
//  		scanner = new Scanner(valueString);
  		
//  		while(scanner.hasNextLine()){
//  			String newLine = scanner.nextLine();    
  			String newLine = value.toString();
  			
  			String[] result = newLine.split("@");
  			String author = result[0].trim();
  			String unigram = result[1];
  			
  			if (result.length>4){
  				String TotalAuthor = result[2];
  				
  				if(firstTimeFlag==1){
  					firstTimeFlag = 0;
  					totalAuthors = Integer.parseInt(TotalAuthor);
  				}
  				
  				String IDF = result[3];
  				String TFIDF = result[4];
  				
  				
  				String finalValue = author+"@"+TotalAuthor+"@"+IDF+"@"+TFIDF;
  			
//  				System.out.println("Unigram: "+unigram+"  "+finalValue);
  				
  				context.write(new Text(unigram), new Text(finalValue));
  				
  			}else{
  				/**
  				 * 
  				 * result.length <= 3
  				 * 
  				 * */
  				String TF = result[2];
  				
  				String finalValue = author+"@"+TF;
  				
  				String aMysteriousRecord = unigram+"@"+finalValue;
  				
//  				mysteriousRecords.add(aMysteriousRecord);
//  				context.write(new Text(unigram), new Text(finalValue));
  		  		String totalAuthorStr = Integer.toString(totalAuthors);
  		  			
//  		  		for (int i=0;i<mysteriousRecords.size();i++){
  		  			String currentRecord = aMysteriousRecord;
  		  			String currentUnigram = currentRecord.split("@")[0];
  		  			String finalvalue = currentRecord.split("@")[1]+"@"+currentRecord.split("@")[2]+"@"+totalAuthorStr;

//  						System.out.println("Unigram: "+currentUnigram+"  "+finalvalue);
  		  			
  		  			context.write(new Text(currentUnigram), new Text(finalvalue));
//  		  		}
  				
  			}
//  		}
  		
  		

  	}
}