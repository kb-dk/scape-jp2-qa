package eu.scape_project.tb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JP2ValidationJob extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		if (args.length < 2)
			throw new RuntimeException("At least two input parameters are needed, input path and output directory");
		
		System.exit(ToolRunner.run(new JP2ValidationJob(), args));
	}

	public int run(String[] args) throws Exception {
		Configuration configuration = getConf();
		Job job = new Job(configuration, "JPEG 2000 Validation");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(getClass());
		job.setMapperClass(JP2ExtractionMapper.class);
		job.setReducerClass(JP2OutputReducer.class);
		job.setNumReduceTasks(1);

		job.setInputFormatClass(NLineInputFormat.class);
		// will split lines from input file into batches, spawning a mapper for each batch
		int splitsize = Integer.parseInt(configuration.get(JP2ValidationConfiguration.SPLITSIZE, "500"));
		NLineInputFormat.setNumLinesPerSplit(job, splitsize);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : -1;
	}
}