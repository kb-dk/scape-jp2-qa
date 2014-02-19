package eu.scape_project.tb.policy;

import java.util.List;

import eu.scape_project.planning.model.policy.PreservationCase;

public class Policy {
	private String organisation = null;
	private List<PreservationCase> preservationCases = null;
	
	public Policy(String organisation, List<PreservationCase> preservationCases){
		this.organisation = organisation;
		this.preservationCases = preservationCases;
	}
	
	public String getOrganisation() {
		return organisation;
	}

	public List<PreservationCase> getPreservationCases() {
		return preservationCases;
	}
}
