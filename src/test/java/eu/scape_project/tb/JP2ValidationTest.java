package eu.scape_project.tb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import eu.scape_project.model.IntellectualEntity;

public class JP2ValidationTest {
	private String outputDirectory = "target/test-output";

	private String inputFilePath1 = "src/test/resources/sample/mets_adresseavisen1759-1795-06-13-01-0006.jp2.xml";
	private String inputFilePath2 = "src/test/resources/sample/mets_adresseavisen1759-1795-06-13-01-0006_rgb.jp2.xml";
	private String outputFilePath1 = "src/test/resources/sample/metsOutput_adresseavisen1759-1795-06-13-01-0006.jp2.xml";
	private String outputFilePath2 = "src/test/resources/sample/metsOutput_adresseavisen1759-1795-06-13-01-0006_rgb.jp2.xml";
	
	private Text inputMetsDocument1;
	private Text inputMetsDocument2;
	private Text outputMetsDocument1;
	private Text outputMetsDocument2;
	
	private Utilities utils;


	@Before
	public void setup() throws IOException, JAXBException {
		File outputDir = new File(outputDirectory);
		outputDir.mkdirs();
		FileUtils.cleanDirectory(outputDir);
		
		initialise();
		utils = new Utilities();
	}
	
	private void initialise() throws FileNotFoundException, IOException {
        StringWriter writer = null;
        try{
	        writer = new StringWriter();

	        IOUtils.copy(new FileInputStream(new File(inputFilePath1)), writer, "UTF-8");
			inputMetsDocument1 = new Text(writer.toString());
			writer.getBuffer().delete(0, writer.getBuffer().length());
			
	        IOUtils.copy(new FileInputStream(new File(inputFilePath2)), writer, "UTF-8");
			inputMetsDocument2 = new Text(writer.toString());
			writer.getBuffer().delete(0, writer.getBuffer().length());

	        IOUtils.copy(new FileInputStream(new File(outputFilePath1)), writer, "UTF-8");
			outputMetsDocument1 = new Text(writer.toString());
			writer.getBuffer().delete(0, writer.getBuffer().length());

	        IOUtils.copy(new FileInputStream(new File(outputFilePath2)), writer, "UTF-8");
			outputMetsDocument2 = new Text(writer.toString());
        }
        finally {
        	if(writer != null)
        		writer.close();
        }
	}

	@Test
	public void shouldValidateJPEG2000Sucessfully() throws IOException {
		MapDriver<Text, Text, Text, Text> driver = new MapDriver<Text, Text, Text, Text>();
		Configuration configuration = driver.getConfiguration();
		configuration.addResource("config/core-site-local.xml");
		
		driver.setMapper(new JP2ExtractionMapper());
		driver.withInput(new Text("0"), inputMetsDocument1);
		List<Pair<Text,Text>> list = driver.run();
		
		Assert.assertEquals("Output size should be 1",  1, list.size());
		Assert.assertEquals("Output key should be SUCCESS", new Text("SUCCESS"), list.get(0).getFirst());
		
        IntellectualEntity expectedIntellectualEntity = utils.createIntellectualEntity(outputMetsDocument1);
        IntellectualEntity actualIntellectualEntity = utils.createIntellectualEntity(list.get(0).getSecond());
        Assert.assertTrue("Differences between intellectual entities", expectedIntellectualEntity.equals(actualIntellectualEntity));
	}
	
	@Test
	public void shouldFailInValidationOfJPEG2000_shouldBeGreyscaleButIsRGB() throws IOException {
		MapDriver<Text, Text, Text, Text> driver = new MapDriver<Text, Text, Text, Text>();
		Configuration configuration = driver.getConfiguration();
		configuration.addResource("config/core-site-local.xml");
		
		driver.setMapper(new JP2ExtractionMapper());
		driver.withInput(new Text("0"), inputMetsDocument2);
		List<Pair<Text,Text>> list = driver.run();
		
		Assert.assertEquals("Output size should be 1",  1, list.size());
		Assert.assertEquals("Output key should be FAILURE", new Text("FAILURE"), list.get(0).getFirst());
		
        IntellectualEntity expectedIntellectualEntity = utils.createIntellectualEntity(outputMetsDocument2);
        IntellectualEntity actualIntellectualEntity = utils.createIntellectualEntity(list.get(0).getSecond());
        Assert.assertTrue("Differences between intellectual entities", expectedIntellectualEntity.equals(actualIntellectualEntity));
	}

	@Test
	public void shouldReduceIntoOneOutputLine() throws IOException {
		ReduceDriver<Text, Text, Text, Text> driver = new ReduceDriver<Text, Text, Text, Text>();
		Configuration configuration = driver.getConfiguration();
		configuration.addResource("config/core-site-local.xml");
		
		List<Text> values = new ArrayList<Text>();
		values.add(inputMetsDocument1);
		values.add(inputMetsDocument1);
		driver.setReducer(new JP2OutputReducer());
		driver.withInput(new Text("SUCCESS"), values);
		List<Pair<Text,Text>> list = driver.run();
		
		Assert.assertEquals("Output size should be 2",  2, list.size());
		Assert.assertEquals("Output key should be SUCCESS", new Text("SUCCESS"), list.get(0).getFirst());
		Assert.assertEquals("Output key should be SUCCESS", new Text("SUCCESS"), list.get(1).getFirst());
		
        IntellectualEntity expectedIntellectualEntity = utils.createIntellectualEntity(outputMetsDocument1);
        IntellectualEntity actualIntellectualEntity = utils.createIntellectualEntity(list.get(0).getSecond());
        Assert.assertTrue("Differences between intellectual entities", expectedIntellectualEntity.equals(actualIntellectualEntity));
        expectedIntellectualEntity = utils.createIntellectualEntity(outputMetsDocument1);
        actualIntellectualEntity = utils.createIntellectualEntity(list.get(1).getSecond());
        Assert.assertTrue("Differences between intellectual entities", expectedIntellectualEntity.equals(actualIntellectualEntity));
	}
	
	@Test
	public void shouldReduceWithTwoOutputKeys() throws IOException {
		MapReduceDriver<Text, Text, Text, Text, Text, Text> driver = new MapReduceDriver<Text, Text, Text, Text, Text, Text>();
		Configuration configuration = driver.getConfiguration();
		configuration.addResource("config/core-site-local.xml");
		
		driver.setMapper(new JP2ExtractionMapper());
		driver.setReducer(new JP2OutputReducer());
		driver.withInput(new Text("0"), inputMetsDocument1);
		driver.withInput(new Text("1"), inputMetsDocument2);
		List<Pair<Text,Text>> list = driver.run();
		
		Assert.assertEquals("Output size should be 2",  2, list.size());
		Assert.assertEquals("Output key should be FAILURE", new Text("FAILURE"), list.get(0).getFirst());
		Assert.assertEquals("Output key should be SUCCESS", new Text("SUCCESS"), list.get(1).getFirst());
		
        IntellectualEntity expectedIntellectualEntity = utils.createIntellectualEntity(outputMetsDocument2);
        IntellectualEntity actualIntellectualEntity = utils.createIntellectualEntity(list.get(0).getSecond());
        Assert.assertTrue("Differences between intellectual entities", expectedIntellectualEntity.equals(actualIntellectualEntity));
        expectedIntellectualEntity = utils.createIntellectualEntity(outputMetsDocument1);
        actualIntellectualEntity = utils.createIntellectualEntity(list.get(1).getSecond());
        Assert.assertTrue("Differences between intellectual entities", expectedIntellectualEntity.equals(actualIntellectualEntity));
	}
}
