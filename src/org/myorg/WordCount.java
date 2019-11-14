
	package org.myorg;
    
	import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

	import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.conf.*;
	import org.apache.hadoop.io.*;

	import org.apache.hadoop.mapred.join.TupleWritable;
	import org.apache.hadoop.mapreduce.*;
	import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
	import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
	import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
	import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
	
	public class WordCount {
		public static class Pair implements WritableComparable {

		    public Text key1;

		    public Text key2;

		    public Pair() {
		        key1 = new Text();
		        key2 = new Text();
		    }

		    public Pair(String key1, String key2) {
		        this.key1 = new Text(key1);
		        this.key2 = new Text(key2);
		    }

		    public void setKey1(Text key1) {
		        this.key1 = key1;
		    }

		    public void setKey2(Text key2) {
		        this.key2 = key2;
		    }

		    public Text getKey1() {
		        return key1;
		    }

		    public Text getKey2() {
		        return key2;
		    }

		    @Override
		    public boolean equals(Object b) {
		        Pair p = (Pair) b;
		        return p.key1.toString().equals(this.key1.toString())
		                && p.key2.toString().equals(this.key2.toString());
		    }

		    @Override
		    public int hashCode() {
		        final int prime = 31;
		        int result = 1;
		        result = prime * result
		                + ((key1 == null) ? 0 : key1.toString().hashCode());
		        result = prime * result
		                + ((key2 == null) ? 0 : key2.toString().hashCode());
		        return result;
		    }

		    @Override
		    public String toString() {
		        return "(" + key1 + ", " + key2 + ")";
		    }

		    @Override
		    public void readFields(DataInput arg0) throws IOException {
		        key1.readFields(arg0);
		        key2.readFields(arg0);
		    }

		    @Override
		    public void write(DataOutput arg0) throws IOException {
		        key1.write(arg0);
		        key2.write(arg0);
		    }


			@Override
			public int compareTo(Object o) {
				Pair p1 = (Pair)o;
				int k = this.key1.toString().compareTo(p1.key1.toString());

		        if (k != 0) {
		            return k;
		        } else {
		            return this.key2.toString().compareTo(p1.key2.toString());
		        }
			}

		}
	 public static class Map extends Mapper<LongWritable, Text, Pair, DoubleWritable> {
	    private final static DoubleWritable one = new DoubleWritable(1);
	    private Text word = new Text();
	   
	     
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {    
	   
	    String line = value.toString();
	    String[] data  = line.split(" ");
	    
	    for(int i=0;i<data.length -1;i++) {
	    	if(data[i] != null && !data[i].isEmpty()) {
	    		for(int j=i+1; j<data.length;j++) {
	    			if(data[j] != null && !data[j].isEmpty()) {
	    				if(data[i].compareTo(data[j]) != 0) {
			    			context.write(new Pair(data[i],"*") ,one);
			    			context.write(new Pair(data[i],data[j]) ,one);
			    		}else {
			    			break;
			    		}
	    			}
		    		
		    	}
	    	}
	    	
	    }
	    }
	    @Override
	    public void cleanup(Context context) throws IOException, InterruptedException{
	    
	    }
	 }
	     
	 public static class Reduce extends Reducer<Pair, DoubleWritable, Pair, DoubleWritable> {	
		 private double totalSum = 0;
		
		 @Override
	        public void setup(final Reducer.Context context) throws IOException, InterruptedException {
			 	totalSum = 0;
	        }
		 
	    public void reduce(Pair key, Iterable<DoubleWritable> values, Context context)
	      throws IOException, InterruptedException {

	    double sum = 0;
	    for (DoubleWritable val : values) {
	        sum += val.get() ;
	    }
	  
		    if(key.key2.equals(new Text("*"))){
		    	totalSum = sum;
		    }else {	    	
		    	 context.write(key, new DoubleWritable ( sum /totalSum));
		    } 
	    }
	 }
	     
	 public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    //hadoop home
	    System.setProperty("hadoop.home.dir", "D:\\MUM-Courses\\BigData\\hadoop-3.1.0");
	    
	    Job job = new Job(conf, "wordcount");
	    //current class
	    job.setJarByClass(WordCount.class);
	    
	    //out put key and value for reducer
	    job.setOutputKeyClass(Pair.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    	
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

