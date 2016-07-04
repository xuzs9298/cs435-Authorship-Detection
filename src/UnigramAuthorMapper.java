import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UnigramAuthorMapper extends Mapper<Object, Text, Text, Text>{
	
    private final static IntWritable one = new IntWritable(1);
  	
  	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {	
  		
  		String valueString = new String(value.copyBytes(),"UTF-8");
  		
  			String newLine = value.toString().trim();
  			
  			String[] result = newLine.split(";");
  			String author = result[0].split("<--->")[0];
  			String totalAuthors = result[0].split("<--->")[1];
  			
  			String[] tfs = result[1].split("@");
  			for (int i=0; i<tfs.length;i++){
  				String currentTF = tfs[i].split(":")[1];
  				String currentUnigram = tfs[i].split(":")[0];
  				
  				String currentValue = author+"$"+totalAuthors+"$"+currentTF;
  				context.write(new Text(currentUnigram), new Text(currentValue));
  			}
  		
  	}
}
