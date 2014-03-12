package eu.scape_project.tb;

import eu.scape_project.planning.model.measurement.Measure;
import eu.scape_project.planning.model.policy.ControlPolicy;
import eu.scape_project.planning.model.policy.ControlPolicy.ControlPolicyType;
import eu.scape_project.planning.model.policy.ControlPolicy.Modality;
import eu.scape_project.planning.model.policy.ControlPolicy.Qualifier;
import eu.scape_project.tb.policy.Policy;
import eu.scape_project.tb.policy.PolicyComparator;
import eu.scape_project.tb.policy.PolicyReader;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.xml.sax.InputSource;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JP2ExtractionMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final Logger logger = Logger.getLogger(JP2ExtractionMapper.class);


    XPath xPath = XPathFactory.newInstance().newXPath();
    Policy organisationPolicy;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        // read control policy
        String organisationalPolicyFilePath = context.getConfiguration()
                                                     .get(JP2ValidationConfiguration.ORGANISATION_POLICY);
        organisationPolicy = new PolicyReader().readPolicy(organisationalPolicyFilePath);

    }

    @Override
    protected void map(LongWritable key, Text inputFilePath, Context context) throws IOException, InterruptedException {
        Text outputKey = null;

        Process process = null;
        final Text failure = new Text("FAILURE");
        final Text success = new Text("SUCCESS");
        try {
            process = jpylize(inputFilePath, context);

            if (process.exitValue() == 0) {
                // transform jpylyzer output into some generic format that can be compared against a control policy
                List<ControlPolicy> controlPolicyList = generateIndividualPolicy(
                        process.getInputStream(), inputFilePath.toString());
                // verify control policy objectives against generic output format
                PolicyComparator policyComparator = new PolicyComparator();
                boolean match = policyComparator.compare(organisationPolicy, controlPolicyList);
                outputKey = match ? success : failure;

                if (!match) {
                    logger.warn(policyComparator.getLog() + " " + inputFilePath.toString());
                }

            } else {
                outputKey = failure;
                logger.error("Exit code: " + process.exitValue());
            }

        } catch (XPathExpressionException e) {
            outputKey = failure;
            logger.error("Exception in control policy generation, using xpath", e);
        } finally {
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
     * this will be replaced with some converter/transformer tool-specific output -> generic output
     *
     * @param jpylyzerMetadata
     *
     * @return
     * @throws IOException
     * @throws XPathExpressionException
     */
    private List<ControlPolicy> generateIndividualPolicy(InputStream jpylyzerMetadata, String inputPath) throws
                                                                                                         IOException,
                                                                                                         XPathExpressionException {
        List<ControlPolicy> controlPolicyList = new ArrayList<ControlPolicy>();

        StringWriter writer = new StringWriter();
        IOUtils.copy(jpylyzerMetadata, writer, "UTF-8");
        String jpylyzerMetadataAsString = writer.toString();
        try {
            ControlPolicy cp1 = makeControlPolicy(
                    "/jpylyzer/properties/jp2HeaderBox/colourSpecificationBox/meth",
                    jpylyzerMetadataAsString,
                    "http://purl.org/DP/quality/measures#18");
            ControlPolicy cp2 = makeControlPolicy(
                    "/jpylyzer/properties/jp2HeaderBox/colourSpecificationBox/enumCS",
                    jpylyzerMetadataAsString,
                    "http://purl.org/DP/quality/measures#19");
            ControlPolicy cp3 = makeControlPolicy(
                    "/jpylyzer/properties/jp2HeaderBox/imageHeaderBox/bPCDepth",
                    jpylyzerMetadataAsString,
                    "http://purl.org/DP/quality/measures#20");

            controlPolicyList.add(cp1);
            controlPolicyList.add(cp2);
            controlPolicyList.add(cp3);
        } catch (XPathExpressionException e) {
            String loginfo = inputPath  + "; " + jpylyzerMetadataAsString + ";";
            logger.error(loginfo,e);
            throw new RuntimeException(e);
        }

        return controlPolicyList;
    }

    private ControlPolicy makeControlPolicy(String expression, String jpylyzerMetadataAsString, String uri) throws
                                                                                                            XPathExpressionException {
        String value = xPath.evaluate(expression, new InputSource(new StringReader(jpylyzerMetadataAsString)));
        Measure measure3 = new Measure();
        measure3.setUri(uri);
        ControlPolicy cp3 = new ControlPolicy();
        cp3.setMeasure(measure3);
        cp3.setControlPolicyType(ControlPolicyType.FORMAT_OBJECTIVE);
        cp3.setModality(Modality.MUST);
        cp3.setQualifier(Qualifier.EQ);
        cp3.setValue(value);
        return cp3;
    }
}
