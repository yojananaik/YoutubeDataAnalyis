package YT_DT;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class YouTubeDataDriver {
	public static void main(String[] args) throws Exception
	{
	Configuration conf = new Configuration();
	if (args.length != 3)
	{
	System.err.println("Usage: YouTubeDataDriver <input path> <output path>");
	System.exit(-1);
	}
	Job job;
	job=Job.getInstance(conf, "Video Genre");
	job.setJarByClass(YouTubeDataDriver.class);
	
	job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    
    job.setMapperClass(YouTubeDataMapper.class);
    job.setReducerClass(YouTubeDataReducer.class);
    job.setCombinerClass(YouTubeDataCombiner.class);
    
    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
