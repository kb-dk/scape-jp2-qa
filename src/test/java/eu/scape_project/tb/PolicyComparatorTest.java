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

public class PolicyComparatorTest {
	private String organisation = "statsbiblioteket";

	@Test
	public void shouldSucceedComparison() {
		
		Policy organisationPolicy = null;
		ControlPolicy controlPolicy = null;

		{
			organisationPolicy = createOrganisationalPolicy();
			controlPolicy = createControlPolicy();
			
			//no changes to control policy
		}
		
		PolicyComparator policyComparator = new PolicyComparator();
		boolean result;
		{
			result = policyComparator.compare(organisationPolicy, controlPolicy);
		}
		
		{
			Assert.assertTrue("Differences between organisation policy and control policy", result);
			Assert.assertTrue("Log should be empty", policyComparator.getLog().isEmpty());
		}
	}

	@Test
	public void shouldFailDueToDifferentMeasure() {
		
		Policy organisationPolicy = null;
		ControlPolicy controlPolicy = null;

		{
			organisationPolicy = createOrganisationalPolicy();
			controlPolicy = createControlPolicy();

			controlPolicy.getMeasure().setUri("http://purl.org/DP/quality/measures#00");
		}
		
		PolicyComparator policyComparator = new PolicyComparator();
		boolean result;
		{
			result = policyComparator.compare(organisationPolicy, controlPolicy);
		}
		
		{
			Assert.assertFalse("Differences between organisation policy and control policy", result);
			Assert.assertEquals("Differences in log information", "INFO no matching control policy found for http://purl.org/DP/quality/measures#00", policyComparator.getLog().trim());
		}
	}
	
	@Test
	public void shouldFailDueToDifferentQualifier() {
		
		Policy organisationPolicy = null;
		ControlPolicy controlPolicy = null;

		{
			organisationPolicy = createOrganisationalPolicy();
			controlPolicy = createControlPolicy();

			controlPolicy.setQualifier(Qualifier.GE);
		}
		
		PolicyComparator policyComparator = new PolicyComparator();
		boolean result;
		{
			result = policyComparator.compare(organisationPolicy, controlPolicy);
		}
		
		{
			Assert.assertFalse("Differences between organisation policy and control policy", result);
			String message = "ERROR Qualifier does not match: organisational CP has value 'equal', candidate CP has value 'greater or equal'\n"
					+ "INFO no matching control policy found for http://purl.org/DP/quality/measures#20";
			Assert.assertEquals("Differences in log information", message, policyComparator.getLog().trim());
		}
	}
	
	@Test
	public void shouldFailDueToDifferentValue() {
		
		Policy organisationPolicy = null;
		ControlPolicy controlPolicy = null;

		{
			organisationPolicy = createOrganisationalPolicy();
			controlPolicy = createControlPolicy();

			controlPolicy.setValue("different_value");
		}
		
		PolicyComparator policyComparator = new PolicyComparator();
		boolean result;
		{
			result = policyComparator.compare(organisationPolicy, controlPolicy);
		}
		
		{
			Assert.assertFalse("Differences between organisation policy and control policy", result);
			String message = "ERROR Value does not match: organisational CP has value '8', candidate CP has value 'different_value'\n"
					+ "INFO no matching control policy found for http://purl.org/DP/quality/measures#20";
			Assert.assertEquals("Differences in log information", message, policyComparator.getLog().trim());
		}
	}
	
	@Test
	public void shouldFailDueToDifferentModality_organisationalBeingMUST() {
		
		Policy organisationPolicy = null;
		ControlPolicy controlPolicy = null;

		{
			organisationPolicy = createOrganisationalPolicy();
			controlPolicy = createControlPolicy();

			controlPolicy.setModality(Modality.SHOULD);
		}
		
		PolicyComparator policyComparator = new PolicyComparator();
		boolean result;
		{
			result = policyComparator.compare(organisationPolicy, controlPolicy);
		}
		
		{
			Assert.assertFalse("Differences between organisation policy and control policy", result);
			String message = "ERROR Modality does not match: organisational CP has value 'must', candidate CP has value 'should'\n"
					+ "INFO no matching control policy found for http://purl.org/DP/quality/measures#20";
			Assert.assertEquals("Differences in log information", message, policyComparator.getLog().trim());
		}
	}
	
	@Test
	public void shouldFailDueToDifferentModality_organisationalBeingSHOULD() {
		
		Policy organisationPolicy = null;
		ControlPolicy controlPolicy = null;

		{
			organisationPolicy = createOrganisationalPolicy();
			organisationPolicy.getPreservationCases().get(0).getControlPolicies().get(0).setModality(Modality.SHOULD);
			controlPolicy = createControlPolicy();
		}
		
		PolicyComparator policyComparator = new PolicyComparator();
		boolean result;
		{
			result = policyComparator.compare(organisationPolicy, controlPolicy);
		}
		
		{
			Assert.assertFalse("Differences between organisation policy and control policy", result);
			String message = "WARN Modality does not match: organisational CP has value 'should', candidate CP has value 'must'\n"
					+ "INFO no matching control policy found for http://purl.org/DP/quality/measures#20";
			Assert.assertEquals("Differences in log information", message, policyComparator.getLog().trim());
		}
	}
	
	private Policy createOrganisationalPolicy() {
		Policy organisationPolicy;
		ControlPolicy organisationControlPolicy = new ControlPolicy();
		organisationControlPolicy.setControlPolicyType(ControlPolicyType.FORMAT_OBJECTIVE);
		organisationControlPolicy.setModality(Modality.MUST);
		
		Measure organisationMeasure = new Measure();
		organisationMeasure.setUri("http://purl.org/DP/quality/measures#20");
		organisationControlPolicy.setMeasure(organisationMeasure);
		
		organisationControlPolicy.setQualifier(Qualifier.EQ);
		organisationControlPolicy.setValue("8");
		
		List<ControlPolicy> organisationControlPolicyList = new ArrayList<ControlPolicy>();
		organisationControlPolicyList.add(organisationControlPolicy);

		PreservationCase preservationCase = new PreservationCase();
		preservationCase.setControlPolicies(organisationControlPolicyList);
		List<PreservationCase> preservationCases = new ArrayList<PreservationCase>();
		preservationCases.add(preservationCase);
		organisationPolicy = new Policy(organisation, preservationCases);
		return organisationPolicy;
	}

	private ControlPolicy createControlPolicy() {
		ControlPolicy controlPolicy;
		controlPolicy = new ControlPolicy();
		
		controlPolicy.setControlPolicyType(ControlPolicyType.FORMAT_OBJECTIVE);
		controlPolicy.setModality(Modality.MUST);
		
		Measure measure = new Measure();
		measure.setUri("http://purl.org/DP/quality/measures#20");
		controlPolicy.setMeasure(measure);
		
		controlPolicy.setQualifier(Qualifier.EQ);
		controlPolicy.setValue("8");
		return controlPolicy;
	}
}
