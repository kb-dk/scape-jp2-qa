package eu.scape_project.tb;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.xml.sax.InputSource;

import eu.scape_project.planning.model.measurement.Measure;
import eu.scape_project.planning.model.policy.ControlPolicy;
import eu.scape_project.planning.model.policy.ControlPolicy.ControlPolicyType;
import eu.scape_project.planning.model.policy.ControlPolicy.Modality;
import eu.scape_project.planning.model.policy.ControlPolicy.Qualifier;
import eu.scape_project.tb.policy.Policy;
import eu.scape_project.tb.policy.PolicyComparator;
import eu.scape_project.tb.policy.PolicyReader;

public class JP2ExtractionMapper extends Mapper<LongWritable, Text, Text, Text> {
	private static final Logger logger = Logger.getLogger(JP2ExtractionMapper.class);

	@Override
	protected void map(LongWritable key, Text inputFilePath, Context context) throws IOException, InterruptedException {
		Text outputKey = null;

		Process process = null;
		try{
			process = jpylize(inputFilePath, context);
	        
			if (process.exitValue() == 0) {
				// transform jpylyzer output into some generic format that can be compared against a control policy
				List<ControlPolicy> controlPolicyList = generateIndividualPolicy(process.getInputStream(), inputFilePath.toString());

				// read control policy
				String organisationalPolicyFilePath = context.getConfiguration().get(JP2ValidationConfiguration.ORGANISATION_POLICY);
				Policy organisationPolicy = new PolicyReader().readPolicy(organisationalPolicyFilePath);

				// verify control policy objectives against generic output format
				PolicyComparator policyComparator = new PolicyComparator();
				boolean match = policyComparator.compare(organisationPolicy, controlPolicyList);

				outputKey = match ? new Text("SUCCESS") : new Text("FAILURE");
				
				if(!match) {
					logger.warn(policyComparator.getLog() + " " + inputFilePath.toString());
				}

			} else {
				outputKey = new Text("FAILURE");
				logger.error("Exit code: " + process.exitValue());
			}
		
		} catch (XPathExpressionException e) {
			outputKey = new Text("FAILURE");
			logger.error("Exception in control policy generation, using xpath", e);
		}
		finally {
			closeProcess(process);
		}

		context.write(outputKey, inputFilePath);
	}

	private Process jpylize(Text inputFilePath, Context context) throws IOException, InterruptedException {
		
		// execute command
		List<String> commandline = Arrays.asList(
				context.getConfiguration().get(JP2ValidationConfiguration.JPYLYZER_EXECUTABLE),
				inputFilePath.toString());

		ProcessBuilder pb = new ProcessBuilder(commandline);
		Process process = pb.start();
		process.waitFor();
		return process;
	}

	private void closeProcess(Process process) throws IOException {
		if (process != null) {
			if (process.getInputStream() != null)
				process.getInputStream().close();
			if (process.getErrorStream() != null)
				process.getErrorStream().close();
			if (process.getOutputStream() != null)
				process.getOutputStream().close();
			process = null;
		}
	}

	/**
	 * this will be replaced with some converter/transformer tool-specific output -> generic output
	 * @param jpylyzerMetadata
	 * @return
	 * @throws IOException
	 * @throws XPathExpressionException 
	 */
	private List<ControlPolicy> generateIndividualPolicy(InputStream jpylyzerMetadata, String inputPath) throws IOException, XPathExpressionException {
		List<ControlPolicy> controlPolicyList = new ArrayList<ControlPolicy>();
		
		StringWriter writer = new StringWriter();
		IOUtils.copy(jpylyzerMetadata, writer, "UTF-8");
		String jpylyzerMetadataAsString = writer.toString();

		String expression = "/jpylyzer/properties/jp2HeaderBox/colourSpecificationBox/meth";
		try {
			XPath xPath = XPathFactory.newInstance().newXPath();

			String value = xPath.evaluate(expression, new InputSource(new StringReader(jpylyzerMetadataAsString)));
			Measure measure1 = new Measure();
			measure1.setUri("http://purl.org/DP/quality/measures#18");
			ControlPolicy cp1 = new ControlPolicy();
			cp1.setMeasure(measure1);
			cp1.setControlPolicyType(ControlPolicyType.FORMAT_OBJECTIVE);
			cp1.setModality(Modality.MUST);
			cp1.setQualifier(Qualifier.EQ);
			cp1.setValue(value);

			expression = "/jpylyzer/properties/jp2HeaderBox/colourSpecificationBox/enumCS";
			value = xPath.evaluate(expression, new InputSource(new StringReader(jpylyzerMetadataAsString)));
			Measure measure2 = new Measure();
			measure2.setUri("http://purl.org/DP/quality/measures#19");
			ControlPolicy cp2 = new ControlPolicy();
			cp2.setMeasure(measure2);
			cp2.setControlPolicyType(ControlPolicyType.FORMAT_OBJECTIVE);
			cp2.setModality(Modality.MUST);
			cp2.setQualifier(Qualifier.EQ);
			cp2.setValue(value);

			expression = "/jpylyzer/properties/jp2HeaderBox/imageHeaderBox/bPCDepth";
			value = xPath.evaluate(expression, new InputSource(new StringReader(jpylyzerMetadataAsString)));
			Measure measure3 = new Measure();
			measure3.setUri("http://purl.org/DP/quality/measures#20");
			ControlPolicy cp3 = new ControlPolicy();
			cp3.setMeasure(measure3);
			cp3.setControlPolicyType(ControlPolicyType.FORMAT_OBJECTIVE);
			cp3.setModality(Modality.MUST);
			cp3.setQualifier(Qualifier.EQ);
			cp3.setValue(value);

			controlPolicyList.add(cp1);
			controlPolicyList.add(cp2);
			controlPolicyList.add(cp3);
		} catch (XPathExpressionException e) {
			String loginfo = inputPath + "; " + expression + "; " + jpylyzerMetadataAsString + ";";
			logger.error(loginfo);
			throw e;
		}

		return controlPolicyList;
	}	
}
