import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.*;

import java.net.URI;

/*
 * Main class of the TFIDF MapReduce implementation.
 * Author: Tyler Stocksdale
 * Date:   10/18/2017
 */
public class TFIDF {

    public static void main(String[] args) throws Exception {
        // Check for correct usage
        if (args.length != 1) {
            System.err.println("Usage: TFIDF <input dir>");
            System.exit(1);
        }
		
		Set<String> positiveWords = new HashSet<>();
		Set<String> negativeWords = new HashSet<>();
		addWords(positiveWords, "/config/positive", "\\r?\\n");
		addWords(negativeWords, "/config/negative", "\\r?\\n");
		System.out.println("Sizes: Positive words = "+positiveWords.size()+", Negative words = "+negativeWords.size());
		
		while(true){
			//System.out.println("At least it prints "+args[0]);
			
			// Create configuration
			Configuration conf = new Configuration();
			
			// Input and output paths for each job
			Path inputPath = new Path("/input");
			Path wcInputPath = inputPath;
			Path wcOutputPath = new Path("/output/WordCount");
			Path dsInputPath = wcOutputPath;
			Path dsOutputPath = new Path("/output/DocSize");
			Path tfidfInputPath = dsOutputPath;
			Path tfidfOutputPath = new Path("/output/TFIDF");
			
			// Get/set the number of documents (to be used in the TFIDF MapReduce job)
			FileSystem fs = inputPath.getFileSystem(conf);
			FileStatus[] stat = fs.listStatus(inputPath);
			String numDocs = String.valueOf(stat.length);
			conf.set("numDocs", numDocs);
			
			// Delete output paths if they exist
			FileSystem hdfs = FileSystem.get(conf);
			if (hdfs.exists(wcOutputPath))
				hdfs.delete(wcOutputPath, true);
			if (hdfs.exists(dsOutputPath))
				hdfs.delete(dsOutputPath, true);
			if (hdfs.exists(tfidfOutputPath))
				hdfs.delete(tfidfOutputPath, true);
			
			// Create and execute Word Count job
			Job job1 = Job.getInstance(conf, "word count");
			job1.setJarByClass(TFIDF.class);
			job1.setMapperClass(WCMapper.class);
			//job1.setCombinerClass(WCReducer.class);
			job1.setReducerClass(WCReducer.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job1, wcInputPath);
			FileOutputFormat.setOutputPath(job1, wcOutputPath);
			boolean success = job1.waitForCompletion(true);
			
			//hdfs.delete(wcInputPath, true);
				
			// Create and execute Document Size job
			//Executing only if previous job was successful.
			if (success) {
				Job job2 = Job.getInstance(conf, "document size");
				job2.setJarByClass(TFIDF.class);
				job2.setMapperClass(DSMapper.class);
				job2.setReducerClass(DSReducer.class);
				//Using KeyValueTextInputFormat since it is reading the output of a previous reduce job
				job2.setInputFormatClass(KeyValueTextInputFormat.class);
				job2.setOutputKeyClass(Text.class);
				job2.setOutputValueClass(Text.class);
				FileInputFormat.addInputPath(job2, dsInputPath);
				FileOutputFormat.setOutputPath(job2, dsOutputPath);
				success = job2.waitForCompletion(true);
			}
			
			//Create and execute TFIDF job
			//Executing only if previous job was successful.
			if (success) {
				Job job3 = Job.getInstance(conf, "TFIDF");
				job3.setJarByClass(TFIDF.class);
				job3.setMapperClass(TFIDFMapper.class);
				job3.setReducerClass(TFIDFReducer.class);
				//Using KeyValueTextInputFormat since it is reading the output of a previous reduce job
				job3.setInputFormatClass(KeyValueTextInputFormat.class);
				job3.setOutputKeyClass(Text.class);
				job3.setOutputValueClass(Text.class);
				FileInputFormat.addInputPath(job3, tfidfInputPath);
				FileOutputFormat.setOutputPath(job3, tfidfOutputPath);
				success = job3.waitForCompletion(true);
			}
				/************ YOUR CODE HERE ************/
			
			List<String> wordsToBeAnalyzed = new ArrayList<>();
			
			addWords(wordsToBeAnalyzed, "/output/TFIDF/part-r-00000", "\\r?\\n");			
			
			//System.out.println("Negative word contain bad ="+negativeWords.contains("bad"));
			
			int sentimentScore = 0;
			int numberOfWords = 0;
			
			for(int i=0;i<wordsToBeAnalyzed.size();i++){
				numberOfWords++;
				String wordToBeAnalyzed = wordsToBeAnalyzed.get(i);
				wordToBeAnalyzed = wordToBeAnalyzed.toLowerCase().trim();
				System.out.println("Analyzing = "+wordToBeAnalyzed);
				if(positiveWords.contains(wordToBeAnalyzed))
					sentimentScore++;
				else if(negativeWords.contains(wordToBeAnalyzed))
					sentimentScore--;
				//System.out.println("Sentiment = "+sentimentScore);
			}
			if(sentimentScore>0){
				System.out.println("Change = "+(sentimentScore*1.0/numberOfWords)*0.1+" %");
			}
			else if(sentimentScore<0){
				System.out.println("Change = "+(sentimentScore*1.0/numberOfWords)*0.1+" %");
			}
			else{
				System.out.println("No change");
			}

		}
    }
	
	public static void addWords(Collection<String> collection, String path, String splitString){
		// ====== Init HDFS File System Object
		Configuration conf = new Configuration();
		// Set FileSystem URI
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
		// Because of Maven
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		// Set HADOOP user
		System.setProperty("HADOOP_USER_NAME", "anjain2");
		System.setProperty("hadoop.home.dir", "/");
		//Get the filesystem - HDFS
		
		try{
			//System.out.println("In try");
			FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000"), conf);
			
			Path hdfsreadpath = new Path(path);
			FSDataInputStream inputStream = fs.open(hdfsreadpath);
			//Classical input stream usage
			String out = IOUtils.toString(inputStream, "UTF-8");
			String[] words = out.split(splitString);
			for(int i=0;i<words.length;i++){
				String word = words[i];
				//System.out.println("Adding word = "+word);
				if(word != null && !word.equals(""))
					collection.add(word);
			}
		}
		catch(Exception ex){
			//System.out.println("In catch");
			System.out.println("No data yet.");
		}
	}
	
	/*
	 * Creates a (key,value) pair for every word in the document 
	 *
	 * Input:  ( byte offset , contents of one line )
	 * Output: ( (word@document) , 1 )
	 *
	 * word = an individual word in the document
	 * document = the filename of the document
	 */
	public static class WCMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		  FileSplit fileSplit = (FileSplit)context.getInputSplit();
		  //Reading file name to be used in key of output
		  String filename = fileSplit.getPath().getName();
		  StringTokenizer itr = new StringTokenizer(value.toString());
		  while (itr.hasMoreTokens()) {
			word.set(itr.nextToken()+"@"+filename);
			context.write(word, one);
		  }
		  try{
			  //System.out.println("Trying to delete file "+fileSplit.getPath().toString());
			  Configuration conf = new Configuration();
			  FileSystem hdfs = FileSystem.get(conf);
			  hdfs.delete(fileSplit.getPath());
		  }
		  catch(Exception ex){
			  System.out.println("Could not delete file");
		  }
		}
    }

    /*
	 * For each identical key (word@document), reduces the values (1) into a sum (wordCount)
	 *
	 * Input:  ( (word@document) , 1 )
	 * Output: ( (word@document) , wordCount )
	 *
	 * wordCount = number of times word appears in document
	 */
	public static class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		  int sum = 0;
		  for (IntWritable val : values) {
			sum += val.get();
		  }
		  result.set(sum);
		  context.write(key, result);
		}
    }
	
	/*
	 * Rearranges the (key,value) pairs to have only the document as the key
	 *
	 * Input:  ( (word@document) , wordCount )
	 * Output: ( document , (word=wordCount) )
	 */
	public static class DSMapper extends Mapper<Text, Text, Text, Text> {
		private Text wordKey = new Text();
		private Text wordValue = new Text();

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			wordKey.set(key.toString().split("@")[1]);
			wordValue.set(key.toString().split("@")[0]+"="+value.toString());
			context.write(wordKey, wordValue);
		}
    }

    /*
	 * For each identical key (document), reduces the values (word=wordCount) into a sum (docSize) 
	 *
	 * Input:  ( document , (word=wordCount) )
	 * Output: ( (word@document) , (wordCount/docSize) )
	 *
	 * docSize = total number of words in the document
	 */
	public static class DSReducer extends Reducer<Text, Text, Text, Text> {
		
		Text newValue = new Text();
		Text newKey = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		  List<String> words = new ArrayList<>();
		  List<Integer> counts = new ArrayList<>();
		  int docSize = 0;
		  for (Text val : values) {
			String c = (val.toString().split("="))[1];
			words.add(val.toString().substring(0, val.toString().indexOf("=")));
			int count = Integer.parseInt(c);
			counts.add(count);
			docSize += count;
		  }
		  for(int i=0;i<words.size();i++){
			newKey.set(words.get(i)+"@"+key.toString());
			newValue.set(counts.get(i)+"/"+docSize);
			context.write(newKey, newValue);
		  }
		}
    }
	
	/*
	 * Rearranges the (key,value) pairs to have only the word as the key
	 * 
	 * Input:  ( (word@document) , (wordCount/docSize) )
	 * Output: ( word , (document=wordCount/docSize) )
	 */
	public static class TFIDFMapper extends Mapper<Text, Text, Text, Text> {
		private Text wordKey = new Text();
		private Text wordValue = new Text();

		String document = "";
		String word = "";
		String fraction = "";
		
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			word = (key.toString().split("@"))[0];
			document = (key.toString().split("@"))[1];
			fraction = value.toString();
			wordKey.set(word);
			wordValue.set(document+"="+fraction);
			context.write(wordKey, wordValue);
		}
    }

    /*
	 * For each identical key (word), reduces the values (document=wordCount/docSize) into a 
	 * the final TFIDF value (TFIDF). Along the way, calculates the total number of documents and 
	 * the number of documents that contain the word.
	 * 
	 * Input:  ( word , (document=wordCount/docSize) )
	 * Output: ( (document@word) , TFIDF )
	 *
	 * numDocs = total number of documents
	 * numDocsWithWord = number of documents containing word
	 * TFIDF = (wordCount/docSize) * ln(numDocs/numDocsWithWord)
	 *
	 * Note: The output (key,value) pairs are sorted using TreeMap ONLY for grading purposes. For
	 *       extremely large datasets, having a for loop iterate through all the (key,value) pairs 
	 *       is highly inefficient!
	 */
	public static class TFIDFReducer extends Reducer<Text, Text, Text, Text> {
		
		private static int numDocs;
		private Map<Text, Text> tfidfMap = new HashMap<>();
		
		// gets the numDocs value and stores it
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			numDocs = Integer.parseInt(conf.get("numDocs"));
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			List<String> documents = new ArrayList<>();
			List<Integer> counts = new ArrayList<>();
			List<Integer> documentSize = new ArrayList<>();
			
			int numDocsWithWord = 0;
			
			for (Text val : values) {
				//System.out.println("For key "+key.toString()+", value = "+val.toString());
				String document = val.toString().split("=")[0];
				String fraction = val.toString().split("=")[1];
				int wordCount = Integer.parseInt(fraction.split("/")[0]);
				int docSize = Integer.parseInt(fraction.split("/")[1]);
				documents.add(document);
				counts.add(wordCount);
				documentSize.add(docSize);
				numDocsWithWord++;
			}
			for(int i=0;i<documents.size();i++){
				Text newKey = new Text();
				Text newValue = new Text();
				double TFIDF = (double)counts.get(i)/documentSize.get(i) * Math.log((double)numDocs/numDocsWithWord);
				newKey.set(key.toString());
				newValue.set(""+TFIDF);
				//System.out.println("Putting "+newKey.toString()+", "+newValue.toString());
				context.write(newKey, new Text());
			}
		}
		
		/*
		// sorts the output (key,value) pairs that are contained in the tfidfMap
		protected void cleanup(Context context) throws IOException, InterruptedException {
            Map<Text, Text> sortedMap = new TreeMap<Text, Text>(tfidfMap);
			for (Text key : sortedMap.keySet()) {
                context.write(key, sortedMap.get(key));
            }
        }*/
		
    }
}
