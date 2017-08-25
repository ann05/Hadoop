import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Lemma2 {
   public static Map<String, String> hmap = new HashMap<String, String>();
  public static class TokenizerMapper
       extends Mapper<LongWritable, Text, Text, Text>{
        String key2,value2;
    private Text word = new Text();
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] tokens = value.toString().split("\\n+");
      if (tokens.length >= 1) {
            for (int i = 0; i < tokens.length; i++) {
                String[] nexttok =tokens[i].toString().split("\\>");
		if(nexttok.length==2){
			//System.out.println("print: " +nexttok[0]+" "+nexttok[1]);
			String[] nexttok2=nexttok[1].toString().split("\\s+");
			//System.out.println("samp "+nexttok2);
			String pos=nexttok[0];
			       pos+=">";
		        for(int j=0;j<nexttok2.length;j++){
		        	word.set(nexttok2[j]);
		    		nexttok2[j].replaceAll("j","i");
		    		nexttok2[j].replaceAll("v","u");
		    		key2=nexttok2[j];
		    		if(hmap.containsKey(key2)){
		        		value2=hmap.get(key2);
		        		Text val=new Text(value2);
					Text pos2=new Text(pos);
		        		context.write(val,pos2);
		    		}
		    		else{
					Text val=new Text(key2);
					Text pos2=new Text(pos);
		        		context.write(val,pos2);
		    		}
		         }
                }
            }
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
    //private IntWritable result = new IntWritable();
    private Text result = new Text();

  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	String temp="";
      for (Text val : values) {
        temp+=val.toString();
      }
      result.set(temp);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {

    String key="";
    String value="";
    String temp="";
    Scanner file = new Scanner(new File("/home/hadoop/la.lexicon.csv")).useDelimiter(",|\n");
    while(file.hasNext()){
    key=file.next();
    temp=file.next();
    value=file.next();
    hmap.put(key,value);
   }
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Lemma");
    job.setJarByClass(Lemma2.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    //FileInputFormat.addInputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}












