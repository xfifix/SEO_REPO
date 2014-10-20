package vsm;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class VectorSpace {
	
	private static VectorSpace vsInstance;
	private Properties properties;
	private Double theta;
	private Integer size;
	
	//Singleton class - private constructor
	private VectorSpace() {
		loadProperties();
		Document.loadDocuments(properties.getProperty("data.collection_path"),
							   properties.getProperty("config.stop_words_path"),
							   properties.getProperty("config.concept_level"),
							   new Double(properties.getProperty("config.concept_alpha")), 
							   properties.getProperty("config.wsd_type"),
							   new Integer(properties.getProperty("config.wsd_context")),
							   "true".equals(properties.getProperty("config.senses_log")),
							   properties.getProperty("config.senses_log_dir")
		);
//		size = Document.getAllDocuments().size();
//		theta = Math.log(weight(Document.getAllDocuments())) / Math.log(size);
	}
	
	private VectorSpace(Boolean isDump) {
		loadProperties();
		Document.loadFromDump(properties.getProperty("data.results_path") + "/dump",
							  new Double(properties.getProperty("config.concept_alpha")),
							  properties.getProperty("config.concept_level"));
	}
	
//	private Double weight(List<Document> docs){
//		Double totalSim = 0.0;
//		for (int i=0; i<docs.size(); i++){
//			Document docI = docs.get(i);
//			for (int j=i+1; j<docs.size(); j++){
//				Document docJ = docs.get(j);
//				totalSim += docI.getSimilarity(docJ);
//			}
//		}
//		return docs.size() + totalSim;
//	}
	
	public static VectorSpace getInstance() {
		if ( vsInstance == null ){
			vsInstance = new VectorSpace();
		}
		return vsInstance;
	}
	
	public static VectorSpace getDumpInstance() {
		if ( vsInstance == null ){
			vsInstance = new VectorSpace(true);
		}
		return vsInstance;
	}
	
	private void loadProperties(){
		properties = new Properties();
		try {
			properties.load(new FileReader(new File("config/properties")));
		} catch (Exception e) {
			System.out.println("Failed to load properties file!!");
			e.printStackTrace();
		}
	}
	
	public String getProperty(String key){
		return properties.getProperty(key);
	}
	
	public Properties getProperties(){
		return properties;
	}
	
//	private Double density(List<Cluster> clusters){
//		Double result = 0.0;
//		for (Iterator<Cluster> iterator = clusters.iterator(); iterator.hasNext();){
//			Cluster cluster = iterator.next();
//			result += cluster.size() * weight(cluster.documents) / ( size * Math.pow(cluster.size(), theta) );
//		}
//		return result;
//	}
	
	private File emptyResultsDir(){
		File resultsDir = new File(properties.getProperty("data.results_path"));
		for (Iterator<String> iter = Arrays.asList(resultsDir.list()).iterator(); iter.hasNext();){
			new File(resultsDir.getPath() + "/" + iter.next()).delete();
		}
		return resultsDir;
	}
	
	public void runClustering(ClusteringMethod method){
//		return density(method.getClusters());
		
		File resultsDir = new File(properties.getProperty("data.results_path") + "/alpha=" + Document.getConceptAlpha().toString() + ", " + method.parameters().toString());
		resultsDir.mkdir();
		Iterator<Cluster> clustersIterator = method.getClusters().iterator();
//		File resultsDir = emptyResultsDir();
		
		for (int i=1; clustersIterator.hasNext(); i++){
			try {
				OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(new File(resultsDir.getPath() + "/" + i)));
				Iterator<Document> docsIterator = clustersIterator.next().iterator();
				while (docsIterator.hasNext()){
					writer.write(docsIterator.next().getFileName() + "\n");
				}
				writer.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
//		System.out.println("Terms:");
//		System.out.println("======");
//		System.out.println(Term.allToString());
//		
//		System.out.println("Documents:");
//		System.out.println("==========");
//		System.out.println(Document.allToString());
		
//		Document.printAllSimilarities();
		
	}
	
	public void runCoring(Coring coring){
		System.out.println("Running Coring");
		List<Core> cores = coring.run();
//		File resultsDir = emptyResultsDir();
		File resultsDir = new File(properties.getProperty("data.results_path"));
		for (Iterator<Core> coresIterator = cores.iterator(); coresIterator.hasNext();){
			Core core = coresIterator.next();
			try {
				OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(new File(resultsDir.getPath() + "/" + core.getDocument().getFileName())));
				writer.write("filename: " + core.getDocument().getFileName() + "\n");
				writer.write("neighborhood size: " + core.getNeighborhoodSize() + "\n");
				writer.write("\n\n\ntext:\n\n");
				char[] buf = new char[(int)core.getDocument().getFile().length()];
				new FileReader(core.getDocument().getFile()).read(buf);
				writer.write(buf);
				writer.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
}
