package eu.scape_project.tb;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import eu.scape_project.planning.model.measurement.Measure;
import eu.scape_project.planning.model.policy.ControlPolicy;
import eu.scape_project.planning.model.policy.ControlPolicy.ControlPolicyType;
import eu.scape_project.planning.model.policy.ControlPolicy.Modality;
import eu.scape_project.planning.model.policy.ControlPolicy.Qualifier;
import eu.scape_project.planning.model.policy.PreservationCase;
import eu.scape_project.tb.policy.Policy;
import eu.scape_project.tb.policy.PolicyComparator;
import eu.scape_project.tb.policy.PolicyReader;

public class PolicyReaderTest {
	private final String rdfPoliciesFilePath = "src/test/resources/sample/statsbiblioteket_control_policy_jpeg2000.rdf";

	@Test
	public void shouldReadPolicy() throws Exception {
		
		List<ControlPolicy> controlPolicyList = new ArrayList<ControlPolicy>();
		ControlPolicy controlPolicy = new ControlPolicy();
		Policy organisationPolicy = null;

		{
			controlPolicy.setControlPolicyType(ControlPolicyType.FORMAT_OBJECTIVE);
			controlPolicy.setModality(Modality.MUST);
			
			Measure measure = new Measure();
			measure.setUri("http://purl.org/DP/quality/measures#20");
			controlPolicy.setMeasure(measure);
			
			controlPolicy.setQualifier(Qualifier.EQ);
			controlPolicy.setValue("8");
			
			controlPolicyList.add(controlPolicy);
		}
		
		{
			PolicyReader reader = new PolicyReader();
			organisationPolicy = reader.readPolicy(rdfPoliciesFilePath);
		}

		{
			Assert.assertNotNull(organisationPolicy);
			List<PreservationCase> preservationCases = organisationPolicy.getPreservationCases();
			Assert.assertEquals("One case expected", 1, preservationCases.size());
			
			PolicyComparator policyComparator = new PolicyComparator();
			Assert.assertTrue("Differences between policy and list of candidate CP list detected", policyComparator.compare(organisationPolicy, controlPolicyList));
			Assert.assertTrue("Differences between preservation case and list of candidate CP list detected", policyComparator.compare(organisationPolicy.getPreservationCases().get(0), controlPolicyList));

			Assert.assertTrue("Differences between policy and candidate CP detected", policyComparator.compare(organisationPolicy, controlPolicy));
			Assert.assertTrue("Differences between preservation case and candidate CP detected", policyComparator.compare(organisationPolicy.getPreservationCases().get(0), controlPolicy));

			Assert.assertEquals("Log information should not exist", policyComparator.getLog(), "");
		}
	}
}
