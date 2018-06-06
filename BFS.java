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
        @param LongWritable: Input key type
        @param Text: Input value type
        @param LongWritable: Output key type
        @param Text: Output value type
   */
  public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      	  String line = value.toString();
          String[] fields = line.split("\t");
          context.write(new LongWritable(Integer.parseInt(fields[0])), new Text(fields[1]));
        }
  }

  /*
    Reduce class

    Parameters:
        @param LongWritable: Input key type
        @param Text: Input value type
        @param LongWritable: Output key type
        @param Text: Output value type
   */
  public static class Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {
    LongWritable root = new LongWritable(Integer.parseInt("1"));
    @Override
    public void reduce(LongWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

	   	String adj_list = "";

		// For every value in the list of values, create a list of values separated by a space,
		// these values conform the adjacency list of the node
		for (Text value : values) {
			adj_list += String.format("%s ", value);
		}
	    
		String formatted_line;

		// If the key equals 1, it means it's the root, which has distance 0
	    if(key.equals(root)){
	        formatted_line = String.format("0\t%s", adj_list);
		}
		// else, it's another node that is not the root, which hasn't been explored, and it has a distance of infinitum
	    else{
			formatted_line = String.format("%d\t%s", Integer.MAX_VALUE, adj_list);
		}            

           // It will write the lines in the following format: 
           // node_id<tab>distance<tab>adj_list
            context.write(key, new Text(formatted_line));
        }
  }

    public int run(String[] args) throws Exception {
      Job job = Job.getInstance(getConf(), "BFS");
      job.setJarByClass(this.getClass());	
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);
      job.setOutputKeyClass(LongWritable.class);
      job.setOutputValueClass(Text.class);
      return job.waitForCompletion(true) ? 0 : 1;
    }
}
