import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SDetector {
	private static String switchStr="CalCosineSimilarity";
	
	public static void main(String[] args) throws Exception{
		
		Configuration config = new Configuration();
		 
	     if (switchStr.equals("GenerateBaseAttributes")){

			 Job job3 = Job.getInstance(config, "SDetector3");
			 job3.setJarByClass(SDetector.class);
			 
		     job3.setOutputKeyClass(Text.class);
		     job3.setOutputValueClass(Text.class);
		     
		     job3.setNumReduceTasks(50);
		     job3.setMapperClass(AuthorCountMapper.class);
		     job3.setReducerClass(AuthorCountReducer.class);
		     
		     FileInputFormat.addInputPath(job3, new Path(args[2]));
		     FileOutputFormat.setOutputPath(job3, new Path(args[3]));
		     
		     job3.waitForCompletion(true);
		     
		     /**
		      * Forth MapperReucer
		      * */
			 Job job4 = Job.getInstance(config, "SDetector4");
			 job4.setJarByClass(SDetector.class);
			 
		     job4.setOutputKeyClass(Text.class);
		     job4.setOutputValueClass(Text.class);
		     
		     job4.setMapperClass(UnigramAuthorMapper.class);
		     job4.setReducerClass(UnigramByHowManyAuthorsReducer.class);
		     
		     FileInputFormat.addInputPath(job4, new Path(args[3]));
		     FileOutputFormat.setOutputPath(job4, new Path(args[4]));
		     
		     job4.waitForCompletion(true);

	     }
	     else if(switchStr.equals("CalCosineSimilarity"))
	     {
	    	 
			 	Job job = Job.getInstance(config, "SDetector");
			 
			 	job.setJarByClass(SDetector.class);
			 
			 	job.setOutputKeyClass(Text.class);
			 	job.setOutputValueClass(Text.class);
			 	
			 	job.setMapperClass(TokenizerMapper.class);
			 	job.setReducerClass(WordCountReducer.class);
		     
			 	FileInputFormat.addInputPath(job, new Path(args[0]));
			 	FileOutputFormat.setOutputPath(job, new Path(args[1]));
		     
			 	job.waitForCompletion(true);
			
			 	/**
			 	 * Second MapperReucer
			 	 * */
			 	Job job2 = Job.getInstance(config, "SDetector2");

			 	job2.setJarByClass(SDetector.class);
			 
			 	job2.setOutputKeyClass(Text.class);
			 	job2.setOutputValueClass(Text.class);
		     
			 	job2.setMapperClass(MysteriousDocTermFrequencyMapper.class);
			 	job2.setReducerClass(MysteriousDocTermFrequencyReducer.class);
		     
			 	FileInputFormat.addInputPath(job2, new Path(args[1]));
			 	FileOutputFormat.setOutputPath(job2, new Path(args[2]));
			 	
			 	job2.waitForCompletion(true);
	    	 
		    	 /**
		    	  * Third mapper
		    	  *  
		    	  * */
				 Job job3 = Job.getInstance(config, "CalCosineSimilarity");
				 
				 job3.setJarByClass(SDetector.class);
				 
			     job3.setOutputKeyClass(Text.class);
			     job3.setOutputValueClass(Text.class);
			     
			     job3.setMapperClass(CosineSimilarityCalHelperMapper.class);
			     job3.setReducerClass(CosineSimilarityCalHelperReducer.class);
			     
			     FileInputFormat.addInputPath(job3, new Path(args[2]));
			     FileInputFormat.addInputPath(job3, new Path(args[3]));
			     FileOutputFormat.setOutputPath(job3, new Path(args[4]));
			     
			     job3.waitForCompletion(true);
			     
			     /**
			      *  By now in the args[4]:
			      *     baseAuthorName       unigram  TFIDF
			      *        .                    .       .
			      *        .                    .       .
			      *        .                    .       .
			      *     $$$DummyAuthor$$$     unigram  TFIDF
			      *        .                    .       .
			      *        .                    .       .
			      *        .                    .       .
			      * 
			      *   this mapper and reducer used to calculate the length 
			      *   of a specific author's attributes and associate the length
			      *   to the author and unigram
			      * 
			      * */
				 Job job4 = Job.getInstance(config, "CalCosineSimilarity");
				 
				 job4.setJarByClass(SDetector.class);
				 
			     job4.setOutputKeyClass(Text.class);
			     job4.setOutputValueClass(Text.class);
			     
			     job4.setMapperClass(AttributesLengthMapper.class);
			     job4.setReducerClass(AttributesLengthReducer.class);
			     
			     FileInputFormat.addInputPath(job4, new Path(args[4]));
			     FileOutputFormat.setOutputPath(job4, new Path(args[5]));
			     
			     job4.waitForCompletion(true); 
			     
			    
			     /**
			      * 
			      * This Mapreduce take the processed records and do the real
			      * calculation of the cosine similarity with the output from 
			      * the AttributesLengthReducer
			  	  *
			      * */
				 Job job5 = Job.getInstance(config, "CalCosineSimilarity");
				 
				 job5.setJarByClass(SDetector.class);
				 
			     job5.setOutputKeyClass(Text.class);
			     job5.setOutputValueClass(Text.class);
			     
			     job5.setMapperClass(CosineSimilarityCalculatorMapper.class);
			     job5.setReducerClass(CosineSimilarityCalculatorReducer.class);
			     
			     FileInputFormat.addInputPath(job5, new Path(args[5]));
			     FileOutputFormat.setOutputPath(job5, new Path(args[6]));
			     
			     job5.waitForCompletion(true); 
			      
			     
			     /**
			      * 
			      * This Mapreduce take the processed records and do the real
			      * calculation of the cosine similarity with the output from 
			      * the AttributesLengthReducer
			  	  *
			      * */
				 Job job6 = Job.getInstance(config, "CalCosineSimilarity");
				 
				 job6.setJarByClass(SDetector.class);
				 
			     job6.setOutputKeyClass(Text.class);
			     job6.setOutputValueClass(Text.class);
			     
			     job6.setMapperClass(AuthorCosineSimilarityAggregatorMapper.class);
			     job6.setReducerClass(AuthorCosineSimilarityAggregatorReducer.class);
			     
			     FileInputFormat.addInputPath(job6, new Path(args[6]));
			     FileOutputFormat.setOutputPath(job6, new Path(args[7]));
			     
			     job6.waitForCompletion(true); 
			     
			     /**
			      * Top Ten Mapper, Combiner, Reducer
			      * */
				 Job job7 = Job.getInstance(config, "CalCosineSimilarity");

				 job7.setJarByClass(SDetector.class);
				 
			     job7.setOutputKeyClass(Text.class);
			     job7.setOutputValueClass(Text.class);
			     
			     job7.setMapperClass(TopTenMapper.class);
			     job7.setCombinerClass(TopTenCombiner.class);
			     job7.setReducerClass(TopTenReducer.class);
			     
			     FileInputFormat.addInputPath(job7, new Path(args[7]));
			     FileOutputFormat.setOutputPath(job7, new Path(args[8]));
			     
			     job7.setNumReduceTasks(1);
			     
			     job7.waitForCompletion(true);
//			     
		 }	     
	     
	}
}
