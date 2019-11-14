
	package org.myorg;
    
	import java.io.IOException;
import java.lang.reflect.Array;
import java.net.URI;
	import java.util.*;

	import org.apache.hadoop.fs.FileSystem;
	import org.apache.hadoop.fs.Path;
import javafx.util.Pair;
import org.apache.hadoop.conf.*;
	import org.apache.hadoop.io.*;
	import org.apache.hadoop.mapreduce.*;
	import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
	import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
	import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
	import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
	import org.apache.log4j.*;

import com.google.common.collect.Maps;
	
	public class StripeRelative {
	 public static class Map extends Mapper<LongWritable, Text, Text, MapWritable> {
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	   // private HashMap<String, HashMap> resultData = new HashMap<String, HashMap>();
	    HashMap<String, MapWritable> mapResult = Maps.newHashMap();
	    
	    @SuppressWarnings("unlikely-arg-type")
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {    
	    String line = value.toString();
	    String[] data  = line.split(" ");
	    
	    for(int i=0;i<data.length -1;i++) {
	    	if(data[i] != null && !data[i].isEmpty()) {
	    		for(int j=i+1; j<data.length;j++) {
		    		if(data[i].compareTo(data[j]) != 0 && data[j] != null && !data[j].isEmpty()) {
		    			if(mapResult.get(data[i])== null) mapResult.put(data[i], new MapWritable());
		    			
		    			MapWritable currentItem = mapResult.get(data[i]);
		    			if(currentItem == null) {
		    				currentItem = new MapWritable();	
		    			}
		    			
		    			if(currentItem.get(new Text(data[j])) == null) {
		    				currentItem.put(new Text(data[j]), one);
		    			}else {
		    				currentItem.put(new Text(data[j]),new IntWritable(Integer.parseInt(currentItem.get(new Text(data[j])).toString())  + 1) );
		    			}
		    			mapResult.put(data[i],currentItem);
		    		}else {
		    			break;
		    		}
		    	}
	    	}
	    
	    }
	
	    }
	    @Override
	    public void cleanup(Context context) throws IOException, InterruptedException{
	    	for(String key:mapResult.keySet()) {
	    		context.write(new Text(key), mapResult.get(key));
	    	}
	    }
	 }
	     
	 public static class Reduce extends Reducer<Text, MapWritable, Text, MapWritable> {	
		 
		//Need to declare Iterable for MapWritable  
	    public void reduce(Text key, Iterable<MapWritable> values, Context context)
	      throws IOException, InterruptedException {
	
	    final DoubleWritable  sum = new DoubleWritable(0);
	    java.util.Map<String, DoubleWritable> accumulator = new HashMap<>();
	    
	    //Interator only run once time to end and stop
	    //Should add value to new data structure to proceed 
	    values.forEach(m -> {
	    	  for (java.util.Map.Entry<Writable, Writable> entry: m.entrySet()) { 
	  	  	    sum.set(sum.get() + Double.parseDouble(entry.getValue().toString()));
	  	  	    
	  	  	    String productKey = entry.getKey().toString();
	  	    	double productCount = ((IntWritable) entry.getValue()).get();
                if (!accumulator.containsKey(productKey) && productKey != null && !productKey.isEmpty()) {
                    accumulator.put(productKey, new DoubleWritable(productCount));
                } else {
                    DoubleWritable currentCount = accumulator.get(productKey);
                    accumulator.put(productKey, new DoubleWritable(currentCount.get() + productCount) );
                }
	  	    }
	    });
	    
	    MapWritable result = new MapWritable();
        for (java.util.Map.Entry<String, DoubleWritable> entry: accumulator.entrySet()) {
            result.put(new Text(entry.getKey()), new DoubleWritable((entry.getValue().get()/sum.get())));
        }
        
        //return output 
        context.write(key, result);   
	    }
	 }
	 
	     
	 public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    //hadoop home
	    System.setProperty("hadoop.home.dir", "D:\\MUM-Courses\\BigData\\hadoop-3.1.0");
	    
	    Job job = new Job(conf, "StripeRelative");
	    //current class
	    job.setJarByClass(StripeRelative.class);
	    
	    //out put key and value for reducer
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(MapWritable.class);
	     
	    //set mapper and reducer
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	     
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	     
	   //input and output folders in hdfs 
	    FileInputFormat.addInputPath(job, new Path("/testinput3"));
	    FileOutputFormat.setOutputPath(job, new Path("/testoutput3"));
	    
	    //submit job
	    job.waitForCompletion(true);
	 }    
	}

