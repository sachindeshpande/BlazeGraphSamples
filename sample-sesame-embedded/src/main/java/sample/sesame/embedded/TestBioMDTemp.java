package sample.sesame.embedded;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.openrdf.OpenRDFException;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.rio.RDFFormat;

import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;


public class TestBioMDTemp {
	
	protected static final Logger log = Logger.getLogger(TestBioMDTemp.class);

	public static void main(String[] args) throws IOException, OpenRDFException {

		// load journal properties from resources
		Properties props = loadProperties("/blazegraph.properties");

		// instantiate a sail
		final BigdataSail sail = new BigdataSail(props);
		final Repository repo = new BigdataSailRepository(sail);

		try{
			repo.initialize();
	
			loadData(repo, "BIOMD.rdf", "");
			
			String queryString = 
		    		"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
					"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " +
					"PREFIX owl: <http://www.w3.org/2002/07/owl#> " +
					"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> " +
					"PREFIX dc: <http://purl.org/dc/elements/1.1/> " +
					"PREFIX dcterms: <http://purl.org/dc/terms/> " +
					"PREFIX foaf: <http://xmlns.com/foaf/0.1/> " +
					"PREFIX skos: <http://www.w3.org/2004/02/skos/core#> " +
					"PREFIX sbmlrdf: <http://identifiers.org/biomodels.vocabulary#> " +
					
					"SELECT ?speciesid ?name WHERE {  " +
					" <http://identifiers.org/biomodels.db/BIOMD0000000001> sbmlrdf:species ?speciesid .  " +
					" ?speciesid sbmlrdf:name ?name}"; 
			
			TupleQueryResult result = executeSelectQuery(repo, queryString, QueryLanguage.SPARQL);
			
			try {
				while(result.hasNext()){
					
					BindingSet bs = result.next();
					log.info(bs);
					
				}
			} finally {
				result.close();
			}
		} finally {
			repo.shutDown();
		}
	}

	/*
	 * Load a Properties object from a file.
	 */
	public static Properties loadProperties(String resource) throws IOException {
		Properties p = new Properties();
		InputStream is = TestBioMDTemp.class
				.getResourceAsStream(resource);
		p.load(new InputStreamReader(new BufferedInputStream(is)));
		return p;
	}

	/*
	 * Load data from resources into a repository.
	 */
	public static void loadData(Repository repo, String resource, String baseURL)
			throws OpenRDFException, IOException {

		RepositoryConnection cxn = repo.getConnection();
		
		try {
			cxn.begin();
			try {
				InputStream is = TestBioMDTemp.class.
                                       getResourceAsStream(resource);
				if (is == null) {
					throw new IOException("Could not locate resource: " + resource);
				}
				Reader reader = new InputStreamReader(new BufferedInputStream(is));
				try {
					cxn.add(reader, baseURL, RDFFormat.N3);
				} finally {
					reader.close();
				}
				cxn.commit();
			} catch (OpenRDFException ex) {
				cxn.rollback();
				throw ex;
			}
		} finally {
			// close the repository connection
			cxn.close();
		}
	}

	/*
	 * Execute sparql select query.
	 */
	public static TupleQueryResult executeSelectQuery(Repository repo, String query,
			QueryLanguage ql) throws OpenRDFException  {

		RepositoryConnection cxn;
		if (repo instanceof BigdataSailRepository) {
			cxn = ((BigdataSailRepository) repo).getReadOnlyConnection();
		} else {
			cxn = repo.getConnection();
		}

		try {

			final TupleQuery tupleQuery = cxn.prepareTupleQuery(ql, query);
			tupleQuery.setIncludeInferred(true /* includeInferred */);
			return tupleQuery.evaluate();
			
		} finally {
			// close the repository connection
			cxn.close();
		}
	}
}