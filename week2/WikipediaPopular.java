import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.lang.Long.*;
import java.util.regex.Pattern;
import java.util.concurrent.TimeUnit;



public class WikipediaPopular extends Configured implements Tool {

	public static class WikiMapper
	extends Mapper<LongWritable, Text, Text, LongWritable>{

		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
			String line = value.toString();
			// before split, define a pattern
			// Or just use: String[] split(String regex)
			// Pattern word_sep = Pattern.compile("[\\s]+");
			// String[] words_in_line = word_sep.split(line);
			String[] words_in_line = line.split("\\s+");

			// System.out.println("words_in_line text is " + words_in_line[0]);
			// System.out.println("words_in_line text is " + words_in_line[1]);
			// System.out.println("words_in_line text is " + words_in_line[2]);
			// System.out.println("words_in_line text is " + words_in_line[3]);

			String view = words_in_line[3];
			String title = words_in_line[2];
			if (words_in_line[1].equals("en") && !title.equals("Main_Page") && !title.startsWith("Special:")) {
				LongWritable view_count = new LongWritable(Long.parseLong(view));
				Text time = new Text(words_in_line[0]);
				context.write(time, view_count);
			}
			
		}
	}

	


	public static class WikiReducer
	extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable();

		@Override
		public void reduce(Text key, Iterable<LongWritable> values,
				Context context
				) throws IOException, InterruptedException {
			long max = 0;
			for (LongWritable val : values) {
				long value = val.get();
				if (value > max) {
				max = value;
				}
			}
			result.set(max);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "wikipedia popular");
		job.setJarByClass(WikipediaPopular.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(WikiMapper.class);
		// job.setCombinerClass(WikiReducer.class);
		job.setReducerClass(WikiReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}