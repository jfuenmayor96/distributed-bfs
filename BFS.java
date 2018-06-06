import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.log4j.Logger;

import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text; 


public class BFS extends Configured implements Tool {
    
  private static final Logger LOG = Logger.getLogger(BFS.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new BFS(), args);
    System.exit(res);
  }

  /*
    Map class

    Parameters:
        @param IntWritable: Input key type
        @param IntWritable: Input value type
        @param IntWritable: Output key type
        @param IntWritable: Output value type
   */
  public static class Map extends Mapper<IntWritable, IntWritable, IntWritable, IntWritable> {
    public void map(IntWritable key, IntWritable value, Context context)
        throws IOException, InterruptedException {
      //String line = lineText.toString();
      //String[] fields = line.split("\s");
          context.write(value);
        }
  }

  /*
    Reduce class

    Parameters:
        @param IntWritable: Input key type
        @param IntWritable: Input value type
        @param IntWritable: Output key type
        @param IntWritable: Output value type
   */
  public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      //String line = lineText.toString();
      //String[] fields = line.split("\s");
          context.write(values);
        }
  }


}