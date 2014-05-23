package eu.scape_project.tb;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.bind.JAXBException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import eu.scape_project.model.File;
import eu.scape_project.model.File.Builder;
import eu.scape_project.model.IntellectualEntity;
import eu.scape_project.model.Representation;
import eu.scape_project.model.TechnicalMetadata;
import eu.scape_project.planning.model.policy.ControlPolicy;
import eu.scape_project.tb.policy.Policy;
import eu.scape_project.tb.policy.PolicyComparator;
import eu.scape_project.tb.policy.PolicyReader;

public class JP2ExtractionMapper extends Mapper<Text, Text, Text, Text> {
    private static final Logger logger = Logger.getLogger(JP2ExtractionMapper.class);
	private final Text failure = new Text("FAILURE");
	private final Text success = new Text("SUCCESS");

    private Policy organisationPolicy;
    private Utilities utils;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        // read control policy
        String organisationalPolicyFilePath = context.getConfiguration().get(JP2ValidationConfiguration.ORGANISATION_POLICY);
        organisationPolicy = new PolicyReader().readPolicy(organisationalPolicyFilePath);
    	try {
			utils = new Utilities();
		} catch (JAXBException e) {
			logger.error("Error occured during initialization of Utilities", e);
			throw new IOException(e);
		}
    }

    @Override
    protected void map(Text key, Text metsDocument, Context context) throws IOException, InterruptedException {
    	Text outputKey = success;
    	Text outputValue = new Text("");
        
        IntellectualEntity intellectualEntity = utils.createIntellectualEntity(metsDocument);
        String filePath = extractFilePath(intellectualEntity);
        
        String jpylyzerMetadata = jpylize(filePath, context);

        if(jpylyzerMetadata == null) {
        	outputKey = failure;
        	logger.warn("No metadata was found in ");
        }
        else {
	        try {	
	        	//enhance METS with output from jpylyzer process
	        	enhanceWithJpylyzerMetadata(intellectualEntity, jpylyzerMetadata);
	        	outputValue = utils.convertToText(intellectualEntity);
	
	        	// transform jpylyzer output into some generic format that can be compared against a control policy
	            List<ControlPolicy> controlPolicyList = utils.generateIndividualPolicy(jpylyzerMetadata, filePath);
	            
	            // verify control policy objectives against generic output format
	            PolicyComparator policyComparator = new PolicyComparator();
	            boolean match = policyComparator.compare(organisationPolicy, controlPolicyList);
	
	            if (!match) {
	            	outputKey = failure;
	                logger.warn(policyComparator.getLog() + " " + filePath);
	            }
	        } catch (XPathExpressionException e) {
	            outputKey = failure;
	            logger.error("Exception in control policy generation, using xpath", e);
	        }
	        catch (ParserConfigurationException e) {
	            outputKey = failure;
	            logger.error("Exception in METS document enhancement", e);
			} catch (SAXException e) {
	            outputKey = failure;
	            logger.error("Exception in METS document enhancement", e);
			}
        }

        context.write(outputKey, outputValue);
    }

    /**
     * Will parse an intellectual entity in search for a file path, only one file path is expected.
     * If zero or more than one file path is found, warnings will be logged.
     * @param intellectualEntity intellectual containing file paths
     * @return the first file path found; a null value is returned, if none can be found
     */
	private String extractFilePath(IntellectualEntity intellectualEntity) {
    	String filePath = null;
    	
    	//08052014: only expect one entry to be added from metsDocument, this could change in the future
    	ArrayList<String> filePathList = new ArrayList<String>();
    	
		List<Representation> representations = intellectualEntity.getRepresentations();
		for(Representation representation : representations){
			List<File> files = representation.getFiles();
			for(File file : files){
				filePathList.add(file.getUri().getPath());
			}
    	}
    	
        if(filePathList.isEmpty())
        	logger.warn("No uri was found in METS document");//TODO include METS document id
        else if(filePathList.size() > 1){
        	filePath = filePathList.get(0);
        	logger.warn("More than one uri was found in METS document");//TODO include METS document id
        }
        else
        	filePath = filePathList.get(0);
        
		return filePath;
	}

	/**
	 * Will execute a jpylyzer process with the jp2 file as input, in order to generate metadata for the jp2 file
	 * @param filePath file path pointing at the jp2 file
	 * @param context mapper context where configuration is found
	 * @return output from jpylyzer process, which is metadata for the jp2 file
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private String jpylize(String filePath, Context context) throws IOException, InterruptedException {

		String jpylyzerMetadata = null;
		Process process = null;
		try {
	        // execute command
	        List<String> commandline = Arrays.asList(
	                context.getConfiguration().get(JP2ValidationConfiguration.JPYLYZER_EXECUTABLE),
	                filePath);
	
	        ProcessBuilder pb = new ProcessBuilder(commandline);
	        process = pb.start();
	        process.waitFor();
	        
            if (process.exitValue() != 0)
            	logger.error("Exit code: " + process.exitValue() + "; " + filePath);
            else {
		        StringWriter writer = null;
		        try{
			        writer = new StringWriter();
			        IOUtils.copy(process.getInputStream(), writer, "UTF-8");
			        jpylyzerMetadata = writer.toString();
		        }
		        finally {
		        	if(writer != null)
		        		writer.close();
		        }
            }
		}
		finally {
			closeProcess(process);
		}

        return jpylyzerMetadata;
    }

	/**
	 * Will close process and corresponding streams (helper method)
	 * @param process
	 * @throws IOException
	 */
    private void closeProcess(Process process) throws IOException {
        if (process != null) {
            if (process.getInputStream() != null) {
                process.getInputStream().close();
            }
            if (process.getErrorStream() != null) {
                process.getErrorStream().close();
            }
            if (process.getOutputStream() != null) {
                process.getOutputStream().close();
            }
            process = null;
        }
    }

    /**
     * Will enhance intellectual entity, actually the file object, with metadata for the referenced image
     * @param intellectualEntity intellectual entity to be enhanced
     * @param jpylyzerMetadata metadata to be stored within intellectual entity
     * @throws ParserConfigurationException
     * @throws SAXException
     * @throws IOException
     */
    private void enhanceWithJpylyzerMetadata(IntellectualEntity intellectualEntity, String jpylyzerMetadata) throws ParserConfigurationException, SAXException, IOException {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		factory.setNamespaceAware(true);
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document doc = builder.parse(new InputSource(new StringReader(jpylyzerMetadata)));
    	TechnicalMetadata technicalMetadata = new TechnicalMetadata("jpylyzer", doc.getDocumentElement());

    	//jpylize has been executed with success, so we know that at least one File object exists
		File file = intellectualEntity.getRepresentations().get(0).getFiles().get(0);
    	Builder filebuilder = new eu.scape_project.model.File.Builder(file);
    	filebuilder.technical(technicalMetadata);
    	intellectualEntity.getRepresentations().get(0).getFiles().set(0, filebuilder.build());
	}
}
