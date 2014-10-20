package vsm;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import rita.wordnet.RiWordnet;

@SuppressWarnings({ "deprecation" })
public class Main {

	public static void main(String[] args) {
		Date start = new Date();
		VectorSpace vs = VectorSpace.getInstance();
		
//		vs.runClustering(new CompleteLinkage(new Integer(vs.getProperty("data.hac_size"))));
		
//		vs.runClustering(new UPGMA(new Integer(vs.getProperty("data.hac_size"))));
		List<Double> alphas = Arrays.asList(0.0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1.0);
		for (Double alpha : alphas){
			Document.resetAllSimilarities(alpha);
			vs.runClustering(new Kmeans(new Integer(vs.getProperty("data.kmeans_size"))));
		}

		alphas = Arrays.asList(0.0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1.0);
		for (Double alpha : alphas){
			Document.resetConceptSetting(alpha);
			vs.runClustering(new SingleLinkage(new Integer(vs.getProperty("data.hac_size"))));
		}
//		vs.runClustering(new DBScan(new Double(vs.getProperty("data.dbscan_threshold")), new Integer(vs.getProperty("data.dbscan_minPoints"))));
		
//		vs.runCoring(new Coring(new Double(vs.getProperty("coring.neighborhood")), new Integer(vs.getProperty("coring.size_threshold"))));
//		vs.runClustering(new CoreClustering(new Double(vs.getProperty("coring.neighborhood")), new Integer(vs.getProperty("coring.size_threshold"))));
		
//		Double density;
		
		//Core clustering, multiple parameters
//		ClusteringMethod method;
//		List<Double> neighborhoods = new ArrayList<Double>();
//		neighborhoods.add(0.35);
//		neighborhoods.add(0.4);
//		neighborhoods.add(0.5);
//		neighborhoods.add(0.4);
//		neighborhoods.add(0.45);
//		neighborhoods.add(0.35);
//		neighborhoods.add(0.3);
//		neighborhoods.add(0.5);
//		neighborhoods.add(0.3);
//		neighborhoods.add(0.45);
//		neighborhoods.add(0.5);
//		neighborhoods.add(0.45);
//		neighborhoods.add(0.4);
//		neighborhoods.add(0.45);
//		neighborhoods.add(0.5);
//		List<Integer> thresholds = new ArrayList<Integer>();
//		thresholds.add(17);
//		thresholds.add(14);
//		thresholds.add(17);
//		thresholds.add(17);
//		thresholds.add(17);
//		thresholds.add(11);
//		thresholds.add(17);
//		thresholds.add(8);
//		thresholds.add(14);
//		thresholds.add(14);
//		thresholds.add(14);
//		thresholds.add(11);
//		thresholds.add(11);
//		thresholds.add(8);
//		thresholds.add(11);
		try {
//			OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(new File(vs.getProperties().getProperty("data.results_path") + "/test_results.txt")));
//			Iterator<Integer> thresholdsIterator = thresholds.iterator();
//			for (Iterator<Double> neighborhoodsIterator = neighborhoods.iterator(); neighborhoodsIterator.hasNext();){
//				Integer threshold = thresholdsIterator.next();
//				Double neighborhood = neighborhoodsIterator.next();
//				method = new CoreClustering(neighborhood, threshold);
//				vs.runClustering(method);
//				System.out.println("At threshold = " + neighborhood + ", minPoints = " + threshold + ": Number of clusters = " + method.clusters.size()/* + ", Expected Density = " + density*/);
//				writer.write("At threshold = " + neighborhood + ", minPoints = " + threshold + ": Number of clusters = " + method.clusters.size()/* + ", Expected Density = " + density*/);
//				writer.write("\n");
//			}
//			writer.close();
//			RiWordnet wn = new RiWordnet();
//			List<WordNetSense> senses = WordNetSense.getSenses("book");
//			for (WordNetSense sense : senses){
//				System.out.print("set:");
//				String[] syns = wn.getSynset(sense.getSenseId());
//				if (syns == null){
//					System.out.print("null");
//				} else {
//					for (String syn : syns){
//						System.out.print(syn + "  ");
//					}
//				}
//				System.out.print(", hyper-hypo-nyms:  ");
//				List<WordNetSense> hyperHypoSenses = sense.getHyperHypoSenses();
//				for (WordNetSense hhsense : hyperHypoSenses){
//					syns = wn.getSynset(hhsense.getSenseId());
//					if (syns == null){
//						System.out.print("null");
//					} else {
//						for (String hhnym : syns){
//							System.out.print(hhnym + "  ");
//						}
//					}
//					System.out.print(", ");
//				}
//				System.out.println();
//			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Date end = new Date();
		System.out.print("completed execution in ");
		System.out.print(end.getHours() - start.getHours());
		System.out.print("hours, ");
		System.out.print(end.getMinutes() - start.getMinutes());
		System.out.print("minutes, ");
	}

}
