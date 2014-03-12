package eu.scape_project.tb;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JP2ValidationTest {
	private Text inputFilePath1 = new Text("src/test/resources/sample/adresseavisen1759-1795-06-13-01-0006.jp2");
	private Text inputFilePath2 = new Text("src/test/resources/sample/adresseavisen1759-1795-06-13-01-0006_rgb.jp2");
	private String outputDirectory = "target/test-output";

	@Before
	public void setup() throws IOException {
		File outputDir = new File(outputDirectory);
		outputDir.mkdirs();
		FileUtils.cleanDirectory(outputDir);
	}

	@Test
	public void shouldValidateJPEG2000Sucessfully() throws IOException {
		MapDriver<LongWritable, Text, Text, Text> driver = new MapDriver<LongWritable, Text, Text, Text>();
		Configuration configuration = driver.getConfiguration();
		configuration.addResource("config/core-site-local.xml");
		
		driver.setMapper(new JP2ExtractionMapper());
		driver.withInput(new LongWritable(0), inputFilePath1);
		driver.withOutput(new Text("SUCCESS"), inputFilePath1);
		driver.runTest();
	}
	
	@Test
	public void shouldFailInValidationOfJPEG2000_shouldBeGreyscaleButIsRGB() throws IOException {
		MapDriver<LongWritable, Text, Text, Text> driver = new MapDriver<LongWritable, Text, Text, Text>();
		Configuration configuration = driver.getConfiguration();
		configuration.addResource("config/core-site-local.xml");
		
		driver.setMapper(new JP2ExtractionMapper());
		driver.withInput(new LongWritable(0), inputFilePath2);
		driver.withOutput(new Text("FAILURE"), inputFilePath2);
		driver.runTest();
	}

	@Test
	public void shouldReduceIntoOneOutputLine() throws IOException {
		ReduceDriver<Text, Text, Text, Text> driver = new ReduceDriver<Text, Text, Text, Text>();
		Configuration configuration = driver.getConfiguration();
		configuration.addResource("config/core-site-local.xml");
		
		List<Text> values = new ArrayList<Text>();
		values.add(inputFilePath1);
		values.add(inputFilePath1);
		driver.setReducer(new JP2OutputReducer());
		driver.withInput(new Text("SUCCESS"), values);
		driver.withOutput(new Text("SUCCESS"), new Text(inputFilePath1 + "\n" + inputFilePath1));
		driver.runTest();
	}
	
	@Test
	public void shouldReduce() throws IOException {
		MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> driver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, Text>();
		Configuration configuration = driver.getConfiguration();
		configuration.addResource("config/core-site-local.xml");
		
		driver.setMapper(new JP2ExtractionMapper());
		driver.setReducer(new JP2OutputReducer());
		driver.withInput(new LongWritable(0), inputFilePath1);
		driver.withInput(new LongWritable(1), inputFilePath2);
		driver.withOutput(new Text("FAILURE"), new Text(inputFilePath2));
		driver.withOutput(new Text("SUCCESS"), new Text(inputFilePath1));
		driver.runTest();
	}
}
