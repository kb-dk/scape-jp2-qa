package eu.scape_project.tb.policy;

import java.util.Iterator;
import java.util.List;

import eu.scape_project.planning.model.policy.ControlPolicy;
import eu.scape_project.planning.model.policy.ControlPolicy.Modality;
import eu.scape_project.planning.model.policy.PreservationCase;

public class PolicyComparator {
	
	private StringBuffer log;
	
	public PolicyComparator() {
		log = new StringBuffer();
	}
	
	public String getLog() {
		return log.toString();
	}

	/**
	 * 
	 * @param organisationPolicy policy for specific organisation 
	 * @param controlPolicyList list of control policies to look for in the policy
	 * @return true, if all control policies in controlPolicyList are matched in organisationPolicy; otherwise false
	 */
	public boolean compare(Policy organisationPolicy, List<ControlPolicy> controlPolicyList) {
		//TODO either loop through all preservation cases, or pick one particular based on its name (name should be another input parameter)
		PreservationCase preservationCase = organisationPolicy.getPreservationCases().get(0);
		return compare(preservationCase, controlPolicyList);
	}
	
	/**
	 * 
	 * @param organisationPolicy policy for specific organisation
	 * @param candidate control policy to look for in the policy
	 * @return true, if candidate control policy is matched in organisationPolicy; otherwise false
	 */
	public boolean compare(Policy organisationPolicy, ControlPolicy candidate) {
		// TODO either loop through all preservation cases, or pick one particular based on its name (name should be another input parameter)
		PreservationCase preservationCase = organisationPolicy.getPreservationCases().get(0);
		return compare(preservationCase, candidate);
	}

	/**
	 * 
	 * @param preservationCase preservation case for some organisation
	 * @param controlPolicyList list of control policies to look for in the preservation case
	 * @return true, if all control policies in controlPolicyList are matched in preservationCase; otherwise false
	 */
	public boolean compare(PreservationCase preservationCase, List<ControlPolicy> controlPolicyList){
		boolean exactMatch = true;
		
		//for each control policy, check that an equal control policy exists in organisation policy
		for(ControlPolicy candidate : controlPolicyList)
			exactMatch &= compare(preservationCase, candidate);
		
		return exactMatch;
	}

	/**
	 * 
	 * @param preservationCase preservation case for some organisation
	 * @param candidate control policy to look for in the preservationCase
	 * @return true, if candidate control policy is matched in preservationCase; otherwise false
	 */
	public boolean compare(PreservationCase preservationCase, ControlPolicy candidate) {
		boolean found = false;
		Iterator<ControlPolicy> iterator = preservationCase.getControlPolicies().iterator();
		while (!found && iterator.hasNext()) {
			ControlPolicy base = iterator.next();
			found = match(base, candidate);
		}

		if (!found)
			info(String.format("no matching control policy found for %s", candidate.getMeasure().getUri()));

		return found;
	}
	
	/**
	 * 
	 * @param organisationalControlPolicy
	 * @param candidateControlPolicy
	 * @return
	 */
	private boolean match(ControlPolicy organisationalControlPolicy, ControlPolicy candidateControlPolicy) {
		String doesNotMatch = "does not match: organisational CP has value '%s', candidate CP has value '%s'";
		boolean match = false;
		
		if (organisationalControlPolicy.getMeasure().compareTo(candidateControlPolicy.getMeasure()) == 0) {
			match = true;

			//TODO currently ControlPolicyType for organisational CP is null
			//ControlPolicyType
//			if(match && organisationalControlPolicy.getControlPolicyType().compareTo(candidate.getControlPolicyType()) != 0) {
//				match = false;
//				error(String.format("ControlPolicyType does not match: organisational CP has value '%s', candidate CP has value '%s'", organisationalControlPolicy.getControlPolicyType(), candidate.getControlPolicyType()));
//			}
			

			//Modality
			if(organisationalControlPolicy.getModality().compareTo(candidateControlPolicy.getModality()) != 0) {
				match = false;
				if(organisationalControlPolicy.getModality().equals(Modality.SHOULD))
					warn(String.format("Modality " + doesNotMatch, organisationalControlPolicy.getModality(), candidateControlPolicy.getModality()));
				else
					error(String.format("Modality " + doesNotMatch, organisationalControlPolicy.getModality(), candidateControlPolicy.getModality()));
			}
			
			//Qualifier			
			if(organisationalControlPolicy.getQualifier().compareTo(candidateControlPolicy.getQualifier()) != 0) {
				match = false;
				error(String.format("Qualifier " + doesNotMatch, organisationalControlPolicy.getQualifier(), candidateControlPolicy.getQualifier()));
			}
			
			//Value
			if(organisationalControlPolicy.getValue().compareTo(candidateControlPolicy.getValue()) != 0) {
				match = false;
				error(String.format("Value " + doesNotMatch, organisationalControlPolicy.getValue(), candidateControlPolicy.getValue()));
			}
			
		}
		
		//CONTROL POLICY
		//enum ControlPolicyType
		//enum Modality
		//enum Qualifier
		//String uri
		//String value
		//Measure measure
		
		//MEASURE
		//String uri
		//Attribute attribute
		//Scale scale
		
		//ATTRIBUTE
		//String uri
		//CriterionCategory category
		
		//SCALE
		
		return match;
	}
	
	private void info(String message) {
		log.append("INFO ");
		log.append(message);
		log.append(String.format("%n"));
	}
	
	private void warn(String message) {
		log.append("WARN ");
		log.append(message);
		log.append(String.format("%n"));
	}

	private void error(String message) {
		log.append("ERROR ");
		log.append(message);
		log.append(String.format("%n"));
	}
}
