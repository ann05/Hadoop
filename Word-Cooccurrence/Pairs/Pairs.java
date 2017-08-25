import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class Pairs{

public static class WordPair implements Writable,WritableComparable<WordPair> {

    private Text word;
    private Text neighbour;

    public WordPair(Text word, Text neighbour) {
        this.word = word;
        this.neighbour = neighbour;
    }

    public WordPair(String word, String neighbour) {
        this(new Text(word),new Text(neighbour));
    }

    public WordPair() {
        this.word = new Text();
        this.neighbour = new Text();
    }

   @Override
    public int compareTo(WordPair other) {
        int returnVal = this.word.compareTo(other.getWord());
        if(returnVal != 0){
            return returnVal;
        }
        if(this.neighbour.toString().equals("*")){
            return -1;
        }else if(other.getNeighbor().toString().equals("*")){
            return 1;
        }
        return this.neighbour.compareTo(other.getNeighbor());
    }

    public static WordPair read(DataInput in) throws IOException {
        WordPair wordPair = new WordPair();
        wordPair.readFields(in);
        return wordPair;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        word.write(out);
        neighbour.write(out);
    }

 
  @Override
    public void readFields(DataInput in) throws IOException {
        word.readFields(in);
        neighbour.readFields(in);
    } 


    @Override
    public String toString() {
        return "{word=["+word+"]"+
               " neighbour=["+neighbour+"]}";
    } 

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WordPair wordPair = (WordPair) o;

        if (neighbour != null ? !neighbour.equals(wordPair.neighbour) : wordPair.neighbour != null) return false;
        if (word != null ? !word.equals(wordPair.word) : wordPair.word != null) return false;

        return true;
    } 

    @Override
    public int hashCode() {
        int result = word != null ? word.hashCode() : 0;
        result = 163 * result + (neighbour != null ? neighbour.hashCode() : 0);
        return result;
    } 

    public void setWord(String word){
        this.word.set(word);
    }
    public void setNeighbor(String neighbour){
        this.neighbour.set(neighbour);
    }

    public Text getWord() {
        return word;
    } 
    public Text getNeighbor() {
        return neighbour;
    }
}

public static class PairsMapper extends Mapper<LongWritable, Text, WordPair, IntWritable> {
    private WordPair pair = new WordPair();
    private IntWritable intw = new IntWritable(1);

  //  @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //int neighbors = context.getConfiguration().getInt("neighbors", 2);
        int n=2,len,start,end;
        String[] tokens = value.toString().split("\\s+");
 	len=tokens.length;
        if (len > 1) {
          for (int i = 0; i < len; i++) {
              pair.setWord(tokens[i]);
             // int start = (i - n < 0) ? 0 : i - n;
	      if((i-n)>0)
		start=i-n;
	      else
 		start=0;
             // int end = (i + n >= len) ? len - 1 : i + n;
     	       if((i+n)<len)
		 end=i+n;
	       else
		 end=len-1;
              for (int j = start; j <= end; j++) {
                  if (j == i) continue;
                   pair.setNeighbor(tokens[j]);
                   context.write(pair, intw);
              }
          }
      }
  }
}

public static class PairsReducer extends Reducer<WordPair,IntWritable,WordPair,IntWritable> {
    private IntWritable total = new IntWritable();
   // @Override
    protected void reduce(WordPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
             count += value.get();
        }
        total.set(count);
        context.write(key,total);
    }
}
public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Pairs");
    job.setJarByClass(Pairs.class);
    job.setMapperClass(PairsMapper.class);
    job.setCombinerClass(PairsReducer.class);
    job.setReducerClass(PairsReducer.class);
    job.setOutputKeyClass(WordPair.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}
