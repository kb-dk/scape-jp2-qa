package eu.scape_project.tb;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import javax.xml.bind.JAXBException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;
import org.xml.sax.InputSource;

import eu.scape_project.model.Identifier;
import eu.scape_project.model.IntellectualEntity;
import eu.scape_project.model.LifecycleState;
import eu.scape_project.model.Representation;
import eu.scape_project.planning.model.measurement.Measure;
import eu.scape_project.planning.model.policy.ControlPolicy;
import eu.scape_project.planning.model.policy.ControlPolicy.ControlPolicyType;
import eu.scape_project.planning.model.policy.ControlPolicy.Modality;
import eu.scape_project.planning.model.policy.ControlPolicy.Qualifier;
import eu.scape_project.util.ScapeMarshaller;

public class Utilities {
	private static final Logger logger = Logger.getLogger(Utilities.class);
    private final ScapeMarshaller scapeMarshaller;
    private final XPath xPath = XPathFactory.newInstance().newXPath();
	
	public Utilities() throws JAXBException {
		scapeMarshaller = ScapeMarshaller.newInstance();
	}
	
	public IntellectualEntity createIntellectualEntity(Text metsDocument) throws IOException {
		java.io.ByteArrayInputStream input = new java.io.ByteArrayInputStream(metsDocument.toString().getBytes());
		Object deserialize;
		try {
			deserialize = scapeMarshaller.deserialize(IntellectualEntity.class, input);
		} catch (JAXBException e) {
			logger.error("Error occured during deserialization of METS document: " + metsDocument.toString(), e);
			throw new IOException(e);
		}
		
		return (IntellectualEntity)deserialize;
	}

    public Text convertToText(IntellectualEntity intellectualEntity) throws IOException {
    	Text output = null;
    	ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
    	try {
			scapeMarshaller.serialize(intellectualEntity, byteArray);
			output = new Text(byteArray.toString());
		} catch (JAXBException e) {
			logger.error("Error occured during serialization of IntellectualEntity: " + intellectualEntity.getIdentifier().toString(), e);
			throw new IOException(e);
		}
    	
    	return output;
    }

    /**
     * Will be replaced with some converter/transformer tool-specific output -> generic output
     * @param jpylyzerMetadata
     * @param inputPath
     * @return
     * @throws IOException
     * @throws XPathExpressionException
     */
    public List<ControlPolicy> generateIndividualPolicy(String jpylyzerMetadata, String inputPath) throws
                                                                                                         IOException,
                                                                                                         XPathExpressionException {
        List<ControlPolicy> controlPolicyList = new ArrayList<ControlPolicy>();

        try {
            ControlPolicy cp1 = makeControlPolicy(
                    "/jpylyzer/properties/jp2HeaderBox/colourSpecificationBox/meth",
                    jpylyzerMetadata,
                    "http://purl.org/DP/quality/measures#18");
            ControlPolicy cp2 = makeControlPolicy(
                    "/jpylyzer/properties/jp2HeaderBox/colourSpecificationBox/enumCS",
                    jpylyzerMetadata,
                    "http://purl.org/DP/quality/measures#19");
            ControlPolicy cp3 = makeControlPolicy(
                    "/jpylyzer/properties/jp2HeaderBox/imageHeaderBox/bPCDepth",
                    jpylyzerMetadata,
                    "http://purl.org/DP/quality/measures#20");

            controlPolicyList.add(cp1);
            controlPolicyList.add(cp2);
            controlPolicyList.add(cp3);
        } catch (XPathExpressionException e) {
            String loginfo = inputPath  + "; " + jpylyzerMetadata + ";";
            logger.error(loginfo,e);
            throw new RuntimeException(e);
        }

        return controlPolicyList;
    }

    /**
     * Will generate control policy/profile where value is extracted from image metadata
     * @param xPathExpression the xpath expression (path) in the jpylyzer output, which value to use in the ControlPolicy to generate
     * @param metadata
     * @param measureUri
     * @return
     * @throws XPathExpressionException
     */
    private ControlPolicy makeControlPolicy(String xPathExpression, String metadata, String measureUri) throws XPathExpressionException {
        String value = xPath.evaluate(xPathExpression, new InputSource(new StringReader(metadata)));
        Measure measure = new Measure();
        measure.setUri(measureUri);
        ControlPolicy cp = new ControlPolicy();
        cp.setMeasure(measure);
        cp.setControlPolicyType(ControlPolicyType.FORMAT_OBJECTIVE);
        cp.setModality(Modality.MUST);
        cp.setQualifier(Qualifier.EQ);
        cp.setValue(value);
        return cp;
    }

    
//  #################################################################################################################################################    
//  #################################################################################################################################################    
    
    public void readSequenceFile(String inputPath) throws IOException {
//		String inputPath = new File("target/test-output/hadoop/part-r-00000").getPath();

		Configuration configuration = new Configuration();
		Path hadoopPath = new Path(inputPath);
		
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(configuration,
					SequenceFile.Reader.file(hadoopPath));

			Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), configuration);
			Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), configuration);

			while (reader.next(key, value)) {
				System.out.println("key: " + key.toString() + ", value: " + value.toString());
			}
		}
		finally {
			if(reader != null)
				reader.close();
		}
	}

	public void createSequenceFile(String inputPath, String outputPath) throws IOException, URISyntaxException, InterruptedException, JAXBException {

//		File inputPath = new File("src/test/resources/sample");
//		String outputPath = new File("target/test-output/sequencefileWithTwoMETSDocuments").getPath();

		Configuration configuration = new Configuration();
		Path hadoopPath = new Path(outputPath);
		SequenceFile.Writer sequenceFileWriter = null;

		try {
			sequenceFileWriter = SequenceFile.createWriter(configuration,
					SequenceFile.Writer.file(hadoopPath),
					SequenceFile.Writer.keyClass(Text.class),
					SequenceFile.Writer.valueClass(Text.class));

			String[] extensions = { "jp2" };
			Iterator<File> iterateFiles = FileUtils.iterateFiles(new File(inputPath), extensions, false);
			while (iterateFiles.hasNext()) {
				File file = iterateFiles.next();

				List<eu.scape_project.model.File> files = new ArrayList<eu.scape_project.model.File>();
				eu.scape_project.model.File.Builder fileBuilder = new eu.scape_project.model.File.Builder();
				fileBuilder
						.identifier(new Identifier(UUID.randomUUID().toString()))
						.filename(file.getName())
						.uri(file.toURI())
						.mimetype(URLConnection.guessContentTypeFromName(file.getName()))
						.technical("technical", null);
				files.add(fileBuilder.build());

				List<Representation> representations = new ArrayList<Representation>();
				Representation.Builder representationBuilder = new Representation.Builder();
				representationBuilder
						.identifier(new Identifier(UUID.randomUUID().toString()))
						.title("title").files(files);
				representations.add(representationBuilder.build());

				IntellectualEntity.Builder entityBuilder = new IntellectualEntity.Builder();
				entityBuilder
						.identifier(new Identifier(UUID.randomUUID().toString()))
						.lifecycleState(new LifecycleState("lifecyclestatus", LifecycleState.State.NEW))
						.versionNumber(1).representations(representations);

				IntellectualEntity entity = entityBuilder.build();

				ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
				scapeMarshaller.serialize(entity, byteArrayOutputStream);
				sequenceFileWriter.append(new Text(file.getName()), new Text(byteArrayOutputStream.toByteArray()));
			}
		} finally {
			if (sequenceFileWriter != null)
				sequenceFileWriter.close();
		}
	}
}
