package eu.scape_project.tb.policy;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.Syntax;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;

import eu.scape_project.planning.manager.CriteriaManager;
import eu.scape_project.planning.model.measurement.Measure;
import eu.scape_project.planning.model.policy.ControlPolicy;
import eu.scape_project.planning.model.policy.PreservationCase;

public class PolicyReader {
	
	public Policy readPolicy(String policyFilePath) throws IOException {
		Model model = createModelFromContent(policyFilePath);
		String organisation = readOrganisation(model);
		List<PreservationCase> preservationCases = readPreservationCases(model);
		
		Policy policy = new Policy(organisation, preservationCases);
		return policy;
	}

	private Model createModelFromContent(String policyFilePath) throws IOException {

        Model model = ModelFactory.createMemModelMaker().createDefaultModel();
		FileInputStream input = null;
		Reader reader = null;
		try {
			input = new FileInputStream(policyFilePath);
			String content = IOUtils.toString(input, "UTF-8");

			reader = new StringReader(content);
	        model = model.read(reader, null);
		}
		finally {
			if(input != null)
				input.close();
			if(reader != null)
				reader.close();
		}
        
        return model;
	}
	
	private String readOrganisation(Model model){
		String organisation = null;
		
        // query organisation from rdf
        String statement =
            "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
            + "PREFIX org: <http://www.w3.org/ns/org#> " 
            + "PREFIX owl: <http://www.w3.org/2002/07/owl#> "
            + "SELECT ?organisation WHERE { " 
            + "?org rdf:type owl:NamedIndividual ."
            + "?org org:identifier ?organisation } ";

        Query orgQuery = QueryFactory.create(statement, Syntax.syntaxARQ);
        QueryExecution orgQe = QueryExecutionFactory.create(orgQuery, model);
        ResultSet orgResults = orgQe.execSelect();

        try {
            if ((orgResults != null) && (orgResults.hasNext())) {
                QuerySolution orgQs = orgResults.next();
                organisation = orgQs.getLiteral("organisation").toString();
            }
        } finally {
            orgQe.close();
        }
        
        return organisation;
	}
	
	private List<PreservationCase> readPreservationCases(Model model){
		List<PreservationCase> preservationCases = new ArrayList<PreservationCase>();
		
        // query all preservationCases
        String statement = "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
            + "PREFIX skos: <http://www.w3.org/2004/02/skos/core#> "
            + "PREFIX pc: <http://purl.org/DP/preservation-case#> "
            + "SELECT ?preservationcase ?name ?contentset WHERE { "
            + "?preservationcase rdf:type pc:PreservationCase . " 
            + "?preservationcase skos:prefLabel ?name . "
            + "?preservationcase pc:hasContentSet ?contentset } ";

        Query pcQuery = QueryFactory.create(statement, Syntax.syntaxARQ);
        QueryExecution pcQe = QueryExecutionFactory.create(pcQuery, model);
        ResultSet pcResults = pcQe.execSelect();

        try {
    		CriteriaManager criteriaManager = new CriteriaManager();
    		criteriaManager.reload();

    		while ((pcResults != null) && (pcResults.hasNext())) {
                QuerySolution pcQs = pcResults.next();
                PreservationCase pc = new PreservationCase();
                pc.setName(pcQs.getLiteral("name").toString());
                pc.setUri(pcQs.getResource("preservationcase").getURI());
                pc.setContentSet(pcQs.getResource("contentset").getURI());
                preservationCases.add(pc);

                // determine user communities
                statement = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
                    + "PREFIX skos: <http://www.w3.org/2004/02/skos/core#> "
                    + "PREFIX pc: <http://purl.org/DP/preservation-case#> "
                    + "SELECT ?usercommunity WHERE { "
                    + "<" + pc.getUri() + ">" + " pc:hasUserCommunity ?usercommunity } ";

                Query ucQuery = QueryFactory.create(statement, Syntax.syntaxARQ);

                QueryExecution ucQe = QueryExecutionFactory.create(ucQuery, model);
                ResultSet ucResults = ucQe.execSelect();

                try {
                    String ucs = "";
                    while ((ucResults != null) && ucResults.hasNext()) {
                        QuerySolution ucQs = ucResults.next();

                        ucs += "," + ucQs.getResource("usercommunity").getLocalName();
                    }
                    if (StringUtils.isNotEmpty(ucs)) {
                        pc.setUserCommunities(ucs.substring(1));
                    }
                } finally {
                    ucQe.close();
                }

                // query objectives
                statement = "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
                    + "PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#> "
                    + "PREFIX pc: <http://purl.org/DP/preservation-case#> "
                    + "PREFIX skos: <http://www.w3.org/2004/02/skos/core#> "
                    + "PREFIX cp: <http://purl.org/DP/control-policy#> "
                    + "SELECT ?objective ?objective_label ?objectiveType ?measure ?modality ?value ?qualifier WHERE { "
                    + "<" + pc.getUri() + ">" + " pc:hasObjective ?objective . " 
                    + "?objective rdf:type ?objectiveType . "
//                    + "?objectiveType rdfs:subClassOf cp:Objective . "
                    + "?objective skos:prefLabel ?objective_label . " 
                    + "?objective cp:measure ?measure . "
                    + "?objective cp:value ?value . " 
                    + "OPTIONAL {?objective cp:qualifier ?qualifier} . "
                    + "?objective cp:modality ?modality "
                    + "}";

                Query query = QueryFactory.create(statement, Syntax.syntaxARQ);
                QueryExecution qe = QueryExecutionFactory.create(query, model);
                ResultSet results = qe.execSelect();

                try {
            		
                    while ((results != null) && (results.hasNext())) {
                        QuerySolution qs = results.next();
                        ControlPolicy cp = new ControlPolicy();

                        String controlPolicyUri = qs.getResource("objective").getURI();
                        String controlPolicyName = qs.getLiteral("objective_label").toString();
                        String measureUri = qs.getResource("measure").toString();
                        String modality = qs.getResource("modality").getLocalName();
                        String value = qs.getLiteral("value").getString();
                        Resource qualifier = qs.getResource("qualifier");
                        
//                        String objectiveType = qs.getResource("objectiveType").getLocalName();

                        Measure m = criteriaManager.getMeasure(measureUri);

                        cp.setUri(controlPolicyUri);
                        cp.setName(controlPolicyName);
                        cp.setValue(value);
                        cp.setMeasure(m);
                        
//                        cp.setControlPolicyType(ControlPolicyType.valueOf(objectiveType));

                        if (qualifier != null) {
                            cp.setQualifier(ControlPolicy.Qualifier.valueOf(qualifier.getLocalName()));
                        } else {
                            cp.setQualifier(ControlPolicy.Qualifier.EQ);
                        }
                        cp.setModality(ControlPolicy.Modality.valueOf(modality));

                        pc.getControlPolicies().add(cp);
                    }
                } finally {
                    qe.close();
                }
            }
        } finally {
            pcQe.close();
        }
        
        return preservationCases;
	}
}
