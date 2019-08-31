package assignment1;

/**
 * 
 * This class solves the problem posed for Assignment1
 *
 */

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class Assignment1 {

	  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{
		  
	    private final static Text one = new Text();
	    private Text word = new Text();

	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    	
	    	Configuration conf = context.getConfiguration();
			String param = conf.get("numgram");
			int numgram = Integer.parseInt(param);
			//get args[0]
			
	    	StringTokenizer itr = new StringTokenizer(value.toString());
	    	
	    	int total = itr.countTokens();
	    	String[] allword = new String[total];
	    	int j = 0;
	    	while (itr.hasMoreTokens()) {
	    		allword[j] = itr.nextToken();
	    		j++;
	        }
	    	//put all words from stringtoken to custom array.
	    	
	    	for(int i = 0; i < total-numgram+1; i = i+1) {
	    		String givenword = allword[i];
	    		for(int k = 1; k < numgram; k++) {
	    			givenword = givenword + " " + allword[i+k];
	    		}
	    		word.set(givenword);
	    	// question1: ngram. Using for-loop to make ngrams.	
	    		
	    		InputSplit inputSplit = context.getInputSplit();
	    		String fileName = ((FileSplit) inputSplit).getPath().toString();
	    		fileName = fileName.substring(fileName.length()-10, fileName.length());
	    		one.set("1" + fileName);
	    	// get the filename, and combine the name and the count into a string.
	    		
		        context.write(word, one);
	    	}
	    }
	  }

	  public static class IntSumReducer extends Reducer<Text,Text,Text,Text> {
	    private Text result = new Text();

	    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
	      int sum = 0;
	      
	      
	      String quzhi = new String();
	      // Create a string to store the whole string.
	      String wenjian = new String();
	      // Create a string to store the filename.
	      String zuihou = new String();
	      // Create a string to put the final result.
	      	      
	      for (Text val : values) {
	    	  
	    	  quzhi = val.toString();
	    	  int cishu = Integer.parseInt(quzhi.substring(0, 1));
	    	  //Create an integer to calculate the sum.
	    	  sum += cishu;
	    	  
	    	  wenjian = wenjian  + quzhi.substring(1, quzhi.length());
	    	  //combine all filenames for the same Text 'word'.
	    	  zuihou = Integer.toString(sum) + " " + wenjian;
	    	  //Put final result to string 'zuihou'.
	      }
	      
	      result.set(zuihou);	        
	      context.write(key, result);
	    }
	  }
	  
	  public static class IntSumReducer2 extends Reducer<Text,Text,Text,Text> {
		  // Override a new class for the final merge management.
		    private Text result = new Text();

		    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		      int sum = 0;
		      
		      Configuration conf = context.getConfiguration();
		      String param2 = conf.get("minicount");
		      int minicount = Integer.parseInt(param2);
		      // Question2: minicount. Get the args[1].
		      
		      String quzhi = new String();
		      String zuihou = new String();
		      String wenjian = new String();
		      // Same as the code from the class IntSumReducer above.
		      for (Text val : values) {
		    	  quzhi = val.toString();
		    	  int cishu = Integer.parseInt(quzhi.substring(0, 1));
		    	  sum += cishu;
		    	  
		    	  wenjian = wenjian  + quzhi.substring(1, quzhi.length());
		    	  zuihou = Integer.toString(sum) + " " + wenjian;
//		        sum += val.get();
		      }
		      
		      if(sum > minicount - 1) {
		    	  //Using if statement to judge if the count is bigger than minicount. 
			      result.set(zuihou);	      
			      context.write(key, result);
		      }
		    }
		  }

	  public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    
	    conf.set("numgram", args[0]);
	    conf.set("minicount", args[1]);
	    
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(Assignment1.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer2.class);
	    //Using the new override class IntSumReducer2.
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    

	    FileInputFormat.addInputPath(job, new Path(args[2]));
	    FileOutputFormat.setOutputPath(job, new Path(args[3]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}