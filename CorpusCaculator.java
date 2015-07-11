/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */


package hadoop;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

public class CorpusCaculator{

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, Text>{
    
    private final static Text one = new Text();
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	String[] line = value.toString().split("\\s+");
    	long hash = line.hashCode(); 
    	Integer cnt= 0;
    	for(String wd: line){
    		if(wd == "")
    			continue;
    		Text wrd = new Text(cnt.toString() + "\t" + wd);
    		word.set(wrd);
    		one.set("1#"+hash);
    		context.write(word, one); 
    		cnt++;
    	}
    }
  }
  
  public static class IntSumReducer 
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();
    private List<String> cache = new ArrayList<String>();

    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      String sentences = "";
      for (Text val : values) {
    	String tmp = val.toString();
    	String[] v = tmp.toString().split("#");
    	int cnt = Integer.parseInt(v[0]);  	
    	sentences += "%"+v[1];
        sum += cnt;
      }
      
      result.set(""+sum+"#"+sentences);
      context.write(key, result);	
   
    }
    
  }
    
   ///////////////////////////////////////////////////////////
    
    
    public static class CountMapper 
    	extends Mapper<Object, Text, Text, Text>{

    	private static Text one = new Text();
    	private Text word = new Text();
   
    	public void map(Object key, Text value, Context context
    			) throws IOException, InterruptedException {
    		String[] line = value.toString().split("\t");
    		
    		String	k = line[0].trim();// Taking position as the key now. 
    		String rest= line[1].toString()+"#"+line[2].toString();// taking rest of the line as string
    		one.set(rest);
    		word.set(k);
    		context.write(word,one);
    	}
    }	
    
    public static class CountReducer 
    	extends Reducer<Text,Text,Text,Text> {
    	private Text result = new Text();

    	public void reduce(Text key, Iterable<Text> values, 
                    Context context
                    ) throws IOException, InterruptedException {
    		int sum = 0;
    		List<String> cache = new ArrayList<String>();

    		String res ="";
    		for(Text val : values){
    			int sentCount = 0;
    			String tmp = val.toString();
    			cache.add(tmp);
    			res = tmp.toString();
    			String[] v = res.split("#"); 
    			if(v.length == 3){
    				sentCount = Integer.parseInt(v[1]);
    				sum += sentCount; 
    				/* this sum will be the number of sentences with
    				 * at least key number of works, and here key is position*/
    			}
    			
    		}
    		
    		for(String ca: cache){
    			String[] v = ca.toString().split("#");
    			String s = "";
    			if(v.length == 3){
    				s = v[0]+"#"+v[1]+"#"+sum+"#"+v[2];
    			}
    			result.set(""+s);
    			context.write(key, result);
    		}
    		}
    	}
  	
    /////////////////////////////////////////////////////////
    
    public static class ThirdMapper 
		extends Mapper<Object, Text, Text, Text>{

    	private static Text one = new Text();
    	private Text word = new Text();

    	public void map(Object key, Text value, Context context
    			) throws IOException, InterruptedException {
    		String[] line = value.toString().split("\t");
		
    		String	pos = line[0].trim();
    		String[] rest= line[1].toString().split("#");
    		String pair = pos+"%"+rest[0];
    		double num = Double.parseDouble(rest[1]);// frequency of pair
    		double den = Double.parseDouble(rest[2]);// number of sentences
    		double prob = (num/den);
    		String values = pair+"#"+prob;
    		String[] Hashes = rest[3].split("%");
    		for(String hash: Hashes){
    			if(!hash.equals("")){
    				word.set(hash);
    				one.set(values);
    				context.write(word,one);
    			}
    		}
    		
    	}
    }	

    public static class ThirdReducer 
		extends Reducer<Text,Text,Text,Text> {
    	private Text result = new Text();

    	public void reduce(Text key, Iterable<Text> values, 
    			Context context
                	) throws IOException, InterruptedException {
		double sentProb = 1.0; 
		String rest ="";
		for(Text val : values){
			String tmp = val.toString();
			String[] v = tmp.split("#");
			double prob = Double.parseDouble(v[1]);
			rest += "#"+v[0];
			sentProb *= prob;
		}
		String val = sentProb+" "+rest;
		result.set(val);
		context.write(key, result);
		
		}
	}
    
    
    public static class FourthMapper 
		extends Mapper<Object, Text, NullWritable, Text>{

    	private TreeMap<Double, String> top3 = new TreeMap<Double, String>();
    	private static Text one = new Text();
    	private DoubleWritable word = new DoubleWritable();

    	public void map(Object key, Text value, Context context
    			) throws IOException, InterruptedException {
    		String[] line = value.toString().split("\t");
		
    		String k = line[0];
    		String[] rest = line[1].split(" ");
    		String[] sentence = rest[1].split("#");
    		double prob = Double.parseDouble(rest[0]);
    		Map<Integer, String> MapSent= new TreeMap<Integer, String>();
    		for(String pair: sentence){
    			if(!pair.equals("")){
    				String[] v = pair.split("%");
    				if(v.length == 2){
    					int pos = Integer.parseInt(v[0]);
    					MapSent.put(pos, v[1]);
    				}
    			}
    		}
    		String res = "";
    		for(Integer ke: MapSent.keySet()){
    			res += MapSent.get(ke)+" ";
    		}
    		
    		String val = prob+"\t"+res;
    		top3.put(prob,val);
    		if(top3.size() > 3){
    			top3.remove(top3.firstKey());
    		}
//    		one.set(res);
//    		word.set(prob);
//    		context.write(word, one);
    	}
    	
    	protected void cleanup(Context context)throws IOException,
					InterruptedException {
    		
    		for(String val: top3.values()){
    			one.set(val);
    			context.write(NullWritable.get(), one);
    		}
    	}
    }	

    public static class FourthReducer 
		extends Reducer<NullWritable,Text, NullWritable,Text> {
    	private DoubleWritable resKey = new DoubleWritable();
    	private Text result = new Text();
    	public void reduce(DoubleWritable key, Iterable<Text> values, 
    			Context context
                	) throws IOException, InterruptedException {
    	TreeMap<Double, Text> top3 = new TreeMap<Double, Text>();
    	for(Text val: values){
    		String[] rest = val.toString().split("#");
//    		resKey.set(k);
//    		result.set(val);
//    		context.write(key, result);
    		Double prob = Double.parseDouble(rest[0]);
    		top3.put(prob, val);
    		
    		if(top3.size() > 3 ){
    			top3.remove(top3.firstKey());
    		}
    	}
    		
    	for(Text val: top3.values()){
    		context.write(NullWritable.get(), val);
    	}
    	
		}
	}
    
    public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();
    	Configuration conf2 = new Configuration();
    	Configuration conf3 = new Configuration();
    	Configuration conf4 = new Configuration();
    	
    	String username = System.getProperty("user.name");
    	String hdfsPath = "/user/"+username+"/";
    			
    	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    	if (otherArgs.length != 2) {
    		System.err.println("Usage: wordcount <in> <out>");
    		System.exit(2);
    	}	
    	
    	Job job1 = new Job(conf, "sentence prob");
    	job1.setJarByClass(CorpusCaculator.class);
    	job1.setMapperClass(TokenizerMapper.class);
    	job1.setReducerClass(IntSumReducer.class);
    	job1.setOutputKeyClass(Text.class);
    	job1.setOutputValueClass(Text.class);
    	job1.setInputFormatClass(TextInputFormat.class);
    	job1.setOutputFormatClass(TextOutputFormat.class);
    	
        
    	FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
    	FileOutputFormat.setOutputPath(job1, new Path(hdfsPath+"/tmp1"));
    	 	
    	job1.waitForCompletion(true);
    	
    	Job job2 = new Job(conf2, "sentence prob");
    	job2.setJarByClass(CorpusCaculator.class);
    	job2.setMapperClass(CountMapper.class);
    	job2.setReducerClass(CountReducer.class);
    	job2.setOutputKeyClass(Text.class);
    	job2.setOutputValueClass(Text.class);
    	job2.setInputFormatClass(TextInputFormat.class);
    	job2.setOutputFormatClass(TextOutputFormat.class);

//    	ControlledJob cJob2 = new ControlledJob(conf);
//        cJob2.setJob(job2);
//        
    	FileInputFormat.addInputPath(job2, new Path(hdfsPath+"tmp1/"));
    	FileOutputFormat.setOutputPath(job2, new Path(hdfsPath+"tmp2"));
//    	
//    	JobControl jobctrl = new JobControl("jobctrl");
//        jobctrl.addJob(job1);
//        jobctrl.addJob(job2);
//        cJob2.addDependingJob(cJob1);
    	job2.waitForCompletion(true);
    	
    	
    	Job job3 = new Job(conf3, "sentence prob");
    	job3.setJarByClass(CorpusCaculator.class);
    	job3.setMapperClass(ThirdMapper.class);
    	job3.setReducerClass(ThirdReducer.class);
    	job3.setOutputKeyClass(Text.class);
    	job3.setOutputValueClass(Text.class);
    	job3.setInputFormatClass(TextInputFormat.class);
    	job3.setOutputFormatClass(TextOutputFormat.class);

    	FileInputFormat.addInputPath(job3, new Path(hdfsPath+"/tmp2/"));
    	FileOutputFormat.setOutputPath(job3, new Path(hdfsPath+"/tmp3/"));

    	job3.waitForCompletion(true);
    	
    	Job job4 = new Job(conf4, "sentence prob");
    	job4.setJarByClass(CorpusCaculator.class);
    	job4.setMapperClass(FourthMapper.class);
    	job4.setReducerClass(FourthReducer.class);
    	job4.setOutputKeyClass(NullWritable.class);
    	job4.setOutputValueClass(Text.class);
    	job4.setNumReduceTasks(1);
    	job4.setInputFormatClass(TextInputFormat.class);
    	job4.setOutputFormatClass(TextOutputFormat.class);

    	FileInputFormat.addInputPath(job4, new Path(hdfsPath+"/tmp3/"));
    	FileOutputFormat.setOutputPath(job4, new Path(otherArgs[1]));
    	
    	System.exit(job4.waitForCompletion(true) ? 0 : 1);
    }
  }	