package crawl4j.vsm;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BetweenRayonWorkerThread implements Runnable {

	private int current=0;
	private List<String> rayons; 
	private Connection con;
	private Map<String,List<String>> rayons_url = new HashMap<String,List<String>>();
	private ArrayList<URLContentInfo> my_infos = new ArrayList<URLContentInfo>();
	private double total_sum=0;
	private double total_exact_number=0;
	public BetweenRayonWorkerThread(Connection con, int current, List<String> rayons, Map<String,List<String>> rayons_url, String field_to_fetch) throws SQLException{
		this.current=current;
		this.rayons_url=rayons_url;
		this.rayons=rayons;
		System.out.println("Computing metrics for rayon :"+ rayons.get(current));
		this.con = con;
	}



	public void run() {
		buildComparisonMatrix();
		//saveResults();
		System.out.println(Thread.currentThread().getName()+" End");
	}

	private void buildComparisonMatrix(){
		int nb_contents = rayons.size();
		for (int i=current; i< rayons.size(); i++){
			double percentage_done = ((double)i/nb_contents)*100;
			double overall_exact_match = total_exact_number/(nb_contents*(nb_contents+1)/2-nb_contents);
			double overall_bow_similarity = total_sum/(nb_contents*(nb_contents+1)/2-nb_contents);
			System.out.println(Thread.currentThread()+ " percentage done : "+percentage_done+"%");
			//	System.out.println(Thread.currentThread()+this.magasin +"Total sum : "+total_sum);
			//	System.out.println(Thread.currentThread()+this.magasin +"Total number of comparisons : "+(nb_contents*(nb_contents+1)/2-nb_contents));
			System.out.println(Thread.currentThread()+"OVerall average exact match : "+overall_exact_match);
			//	System.out.println(Thread.currentThread()+this.magasin +"Total exact sum : "+total_exact_number);
			//	System.out.println(Thread.currentThread()+this.magasin +"Total number of comparisons : "+(nb_contents*(nb_contents+1)/2-nb_contents));
			System.out.println(Thread.currentThread()+"OVerall average VSM BOW similarity : "+overall_bow_similarity);
	
			List<String> fields_i = rayons_url.get(rayons.get(i));
			
            System.out.println("URLs i"+fields_i.size());

//
//			URLContentInfo infoi = my_infos.get(i);
//			//			String rayoni = infoi.getRayon();
//			//			String magasini = infoi.getMagasin();
//			String contenti = infoi.getContent() == null ? "" : infoi.getContent();
//			//			unique_magasins.add(magasini);
//			//			unique_rayons.add(rayoni);
			for (int j=i+1; j<rayons.size(); j++){
				List<String> fields_j = rayons_url.get(rayons.get(j));
	            System.out.println("URLs j"+fields_j.size());
				cross_lists(fields_i,fields_j);
//				URLContentInfo infoj = my_infos.get(j);
//				//				String rayonj = infoj.getRayon();
//				//				String magasinj = infoj.getMagasin();
//				String contentj = infoj.getContent() == null ? "" : infoj.getContent();
//				//System.out.println(contenti);
//				//System.out.println(contentj);
//				boolean isexactlysimilar = contenti.equals(contentj);
//				double similarity = 0;
//				if ("".equals(contenti) || "".equals(contentj)){
//					if (isexactlysimilar){
//						similarity=1;
//					}
//				} else {
//					similarity = computeSimilarity(contenti,contentj);
//				}
//				//System.out.println("i +"+i+" j + "+j+" = "+similarity);
//				if (isexactlysimilar){
//					total_exact_number++;
//				}
//				total_sum=total_sum+similarity;

			}
		}
		System.out.println("Total sum : "+total_sum);
		System.out.println("Total number of comparisons : "+(nb_contents*(nb_contents+1)/2-nb_contents));
		System.out.println("OVerall average VSM BOW similarity : "+total_sum/(nb_contents*(nb_contents+1)/2-nb_contents));

		System.out.println("Total exact sum : "+total_exact_number);
		System.out.println("Total number of comparisons : "+(nb_contents*(nb_contents+1)/2-nb_contents));
		System.out.println("OVerall average VSM BOW similarity : "+total_exact_number/(nb_contents*(nb_contents+1)/2-nb_contents));

	}

	private void cross_lists(List<String> fields_i,List<String> fields_j){
		for (int k=0;k<fields_i.size();k++){
			String contentk = fields_i.get(k);
			for (int l=0;l<fields_j.size();l++){
				String contentl = fields_j.get(k);
				double similarity = 0;
				boolean isexactlysimilar = contentk.equals(contentl);
				if ("".equals(contentk) || "".equals(contentl)){
					if (isexactlysimilar){
						similarity=1;
					}
				} else {
					similarity = computeSimilarity(contentk,contentl);
				}
				if (isexactlysimilar){
					total_exact_number++;
				}
				total_sum=total_sum+similarity;
				System.out.println("Similarity" + similarity);
			}
		}
	}
	
	private Double computeSimilarity(String text1, String text2) {
		VectorStateSpringRepresentation vs1 =new VectorStateSpringRepresentation(text1);
		VectorStateSpringRepresentation vs2 =new VectorStateSpringRepresentation(text2);
		return cosine_similarity(vs1.getWordFrequencies() , vs2.getWordFrequencies());
	}

	private double cosine_similarity(Map<String, Integer> v1, Map<String, Integer> v2) {
		Set<String> both = new HashSet<String>(v1.keySet());
		both.retainAll(v2.keySet());
		double sclar = 0, norm1 = 0, norm2 = 0;
		for (String k : both) sclar += v1.get(k) * v2.get(k);
		for (String k : v1.keySet()) norm1 += v1.get(k) * v1.get(k);
		for (String k : v2.keySet()) norm2 += v2.get(k) * v2.get(k);
		return sclar / Math.sqrt(norm1 * norm2);
	}

}
