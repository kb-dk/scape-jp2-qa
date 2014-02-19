package eu.scape_project.tb;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JP2OutputReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		StringBuffer buffer = new StringBuffer();
		for (Text text : values)
			buffer.append(text.toString()).append(" ");

		Text value = new Text(buffer.toString().trim());
		context.write(key, value);
	}
}