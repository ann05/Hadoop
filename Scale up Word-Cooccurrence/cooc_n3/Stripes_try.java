import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.*;
import java.io.DataInput;
import java.io.DataOutput;


public class Stripes_try{

public static class MyMapWritable extends MapWritable {
    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        Set<Writable> keySet = this.keySet();

        for (Object key : keySet) {
            str.append("= (" + key.toString() + " , " + this.get(key) + ")");
        }
        return str.toString();
    }
}

public static class StripesMapper extends Mapper<LongWritable,Text,Text,MyMapWritable> {
    private MyMapWritable Map = new MyMapWritable();
    private Text word = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int n=3,start,end,len=0;
        String[] tokens = value.toString().split("\\s+");
	len=tokens.length;
	if(len>1){
	for(int i=0;i<len;i++){
		Map.clear();
		word.set(tokens[i]);
		for(int j=i+1;j<len;j++){
			//if(i==j) continue;
			for(int k=j+1;k<len;k++){
				//if(k==j) continue;
				StringBuilder pair_str =new StringBuilder();
				pair_str.append("{ "+ tokens[j]+","+" tokens[k]"+" } ");
				Text neighbour = new Text(pair_str.toString());
				if(Map.containsKey(neighbour)){
					IntWritable count = (IntWritable)Map.get(neighbour);
				       	count.set(count.get()+1);	}
				else
				{ Map.put(neighbour,new IntWritable(1));
					}
			
			}
		
		}
		context.write(word,Map);
	}
	
    }
}
}

public static class StripesReducer extends Reducer<Text, MyMapWritable, Text, MyMapWritable> {
    private MyMapWritable reduceMap = new MyMapWritable();

    @Override
    protected void reduce(Text key, Iterable<MyMapWritable> values, Context context) throws IOException, InterruptedException {
        reduceMap.clear();
        for (MyMapWritable value : values) {
	    Set<Writable> keys = value.keySet();
            for (Writable key2 : keys) {
            IntWritable count2 = (IntWritable) value.get(key2);
	    if(!reduceMap.containsKey(key2))
		reduceMap.put(key2,count2);
	    else{
	        IntWritable count = (IntWritable) reduceMap.get(key2);
                count.set(count.get() + count2.get());
                }
	    }
        }
        context.write(key, reduceMap);
    }
    }

public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Stripes");
    job.setJarByClass(Stripes_try.class);
    job.setMapperClass(StripesMapper.class);
    job.setCombinerClass(StripesReducer.class);
    job.setReducerClass(StripesReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(MyMapWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}


