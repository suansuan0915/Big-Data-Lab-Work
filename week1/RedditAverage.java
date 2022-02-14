import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import java.util.regex.Pattern; 
import org.json.JSONObject;


public class RedditAverage extends Configured implements Tool {

	// Mapper class
	public static class RedditMapper
	extends Mapper<LongWritable, Text, Text, LongPairWritable>{
		// Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>

		private final static LongWritable one = new LongWritable(1);
		private Text word = new Text();	
		LongPairWritable pair = new LongPairWritable();  
		
		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
			
			JSONObject record = new JSONObject(value.toString());
			
			String subreddit = (String) record.get("subreddit");  // array.get()
			long score = ((Number) record.get("score")).longValue();
			long comment_number = 1;
			pair.set(comment_number, score);
			word.set(subreddit);
			context.write(word, pair);  
			
			}
		}
	

	// Combiner class
	public static class RedditCombiner
	extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {
		// private LongPairWritable result = new LongPairWritable();

	@Override
		public void reduce(Text key, Iterable<LongPairWritable> values,
				Context context
				) throws IOException, InterruptedException {
			long sum_comments = 0;
			long sum_scores = 0;

			for (LongPairWritable val : values) {
				sum_comments+=val.get_0();
				sum_scores+=val.get_1();
			}	
			// result.set(avg_scores);
			LongPairWritable pair_for_each_subreddit = new LongPairWritable(sum_comments, sum_scores);
			context.write(key, pair_for_each_subreddit);
		}
	}


	// Reducer class
	public static class RedditReducer
	extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();

	@Override
		public void reduce(Text key, Iterable<LongPairWritable> values,
				Context context
				) throws IOException, InterruptedException {
			long sum_comments = 0;
			long sum_scores = 0;
			double avg_scores = 0.0;

			for (LongPairWritable val : values) {
				sum_comments+=val.get_0();
				sum_scores+=val.get_1();
			}
			avg_scores = (double)sum_scores/(double)sum_comments;
			result.set(avg_scores);
			context.write(key, result);
		}
	}


	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
		System.exit(res);
	}



	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(RedditAverage.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(RedditMapper.class);
		job.setCombinerClass(RedditCombiner.class);
		job.setReducerClass(RedditReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongPairWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

}
