package crawl4j.vsm;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class InsideMagasinWorkerThread implements Runnable {
	private String magasin;
	private List<String> rayons;
	private String field_to_fetch;
	private Connection con;
	private ArrayList<URLContentInfo> my_infos = new ArrayList<URLContentInfo>();
	private double total_sum=0;
	private double total_exact_number=0;

	public InsideMagasinWorkerThread(Connection con, String magasin,List<String> rayons, String field_to_fetch) throws SQLException{
		this.magasin=magasin;
		this.rayons=rayons;
		this.field_to_fetch=field_to_fetch;
		System.out.println("Computing metrics for magasin :"+ magasin);
		this.con = con;
		PreparedStatement pst = null;
		ResultSet rs = null;
		try {
			// we open the database
			pst = this.con.prepareStatement("SELECT "+this.field_to_fetch+" , URL, VENDOR, MAGASIN, RAYON, PRODUIT FROM CRAWL_RESULTS where magasin='"+this.magasin+"'");
			rs = pst.executeQuery();
			while (rs.next()) {
				URLContentInfo url_info = new URLContentInfo();
				String content = rs.getString(1);
				String my_url = rs.getString(2);
				String my_vendor = rs.getString(3);
				String my_magasin = rs.getString(4);
				String my_rayon = rs.getString(5);
				String my_produit = rs.getString(6);
				url_info.setContent(content);
				url_info.setUrl(my_url);
				url_info.setMagasin(my_magasin);
				url_info.setRayon(my_rayon);
				url_info.setProduit(my_produit);
				url_info.setVendor(my_vendor);
				my_infos.add(url_info);
			}    
		} catch (Exception ex) {
			Logger lgr = Logger.getLogger(ProcessInsideMagasinPerRayonSimilarity.class.getName());
			lgr.log(Level.SEVERE, ex.getMessage(), ex);
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (pst != null) {
					pst.close();
				}
			} catch (SQLException ex) {
				Logger lgr = Logger.getLogger(ProcessInsideMagasinPerRayonSimilarity.class.getName());
				lgr.log(Level.WARNING, ex.getMessage(), ex);
			}
		}	
	}

	public void run() {
		buildComparisonMatrix();
		//saveResults();
		System.out.println(Thread.currentThread().getName()+" End");
	}

	private void buildComparisonMatrix(){
		int nb_contents = my_infos.size();
		System.out.println("Computing similarity distance : "+nb_contents);
		for (int i=0; i< nb_contents; i++){
			double percentage_done = ((double)i/nb_contents)*100;
			double overall_exact_match = total_exact_number/(nb_contents*(nb_contents+1)/2-nb_contents);
			double overall_bow_similarity = total_sum/(nb_contents*(nb_contents+1)/2-nb_contents);
			if (Double.isNaN(overall_exact_match)){
				System.out.println("Trouble");
			}
			if (Double.isNaN(overall_bow_similarity)){
				System.out.println("Trouble");
			}
			if (Double.isNaN(total_exact_number)){
				System.out.println("Trouble");
			}
			System.out.println(Thread.currentThread()+this.magasin + " percentage done : "+percentage_done+"%");
			//	System.out.println(Thread.currentThread()+this.magasin +"Total sum : "+total_sum);
			//	System.out.println(Thread.currentThread()+this.magasin +"Total number of comparisons : "+(nb_contents*(nb_contents+1)/2-nb_contents));
			System.out.println(Thread.currentThread()+this.magasin +"OVerall average exact match : "+overall_exact_match);
			//	System.out.println(Thread.currentThread()+this.magasin +"Total exact sum : "+total_exact_number);
			//	System.out.println(Thread.currentThread()+this.magasin +"Total number of comparisons : "+(nb_contents*(nb_contents+1)/2-nb_contents));
			System.out.println(Thread.currentThread()+this.magasin +"OVerall average VSM BOW similarity : "+overall_bow_similarity);

			URLContentInfo infoi = my_infos.get(i);
			//			String rayoni = infoi.getRayon();
			//			String magasini = infoi.getMagasin();
			String contenti = infoi.getContent() == null ? "" : infoi.getContent();
			//			unique_magasins.add(magasini);
			//			unique_rayons.add(rayoni);
			for (int j=i+1; j<nb_contents; j++){
				URLContentInfo infoj = my_infos.get(j);
				//				String rayonj = infoj.getRayon();
				//				String magasinj = infoj.getMagasin();
				String contentj = infoj.getContent() == null ? "" : infoj.getContent();
				//System.out.println(contenti);
				//System.out.println(contentj);
				boolean isexactlysimilar = contenti.equals(contentj);
				double similarity = 0;
				if ("".equals(contenti) || "".equals(contentj)){
					if (isexactlysimilar){
						similarity=1;
					}
				} else {
					similarity = computeSimilarity(contenti,contentj);
				}
				//System.out.println("i +"+i+" j + "+j+" = "+similarity);
				if (isexactlysimilar){
					total_exact_number++;
				}
				total_sum=total_sum+similarity;

				//				if (similarity>=threshold){
				//					// storing each magasin result
				//					RayonComparison comp = my_rayon_results.get(rayoni+rayonj);
				//					if (comp == null){
				//						comp=new RayonComparison();
				//						comp.setRayoni(rayoni);
				//						comp.setRayonj(rayonj);
				//					}
				//					Double raytot = comp.getTotal();
				//					raytot=raytot+similarity;
				//					comp.setTotal(raytot);
				//					comp.increment();
				//					my_rayon_results.put(rayoni+rayonj, comp);
				//
				//					System.out.println(rayoni);
				//					System.out.println(rayonj);
				//					System.out.println(comp);
				//
				//					// storing each rayon result
				//					MagasinComparison mag_comp = my_magasin_results.get(magasini+magasinj);
				//					if (mag_comp == null){
				//						mag_comp=new MagasinComparison();
				//						mag_comp.setMagasini(magasini);
				//						mag_comp.setMagasinj(magasinj);
				//					}
				//					Double tot = mag_comp.getTotal();
				//					tot=tot+similarity;
				//					mag_comp.setTotal(tot);
				//					mag_comp.increment();
				//
				//					my_magasin_results.put(magasini+magasinj, mag_comp);
				//					System.out.println(magasini);
				//					System.out.println(magasinj);
				//					System.out.println(mag_comp);
				//
				//					// too costly for the time being
				//					//					// storing each element
				//					//					SparseElement my_elem = new SparseElement();
				//					//					my_elem.setPosition_i(i);
				//					//					my_elem.setPosition_j(j);
				//					//					my_elem.setValue(similarity);
				//					System.out.println(contenti);
				//					System.out.println(contentj);
				//					System.out.println("i +"+i+" j + "+j+" = "+similarity);
				//					//					my_url_results.add(my_elem);
				//				}
			}
		}
		System.out.println("Total sum : "+total_sum);
		System.out.println("Total number of comparisons : "+(nb_contents*(nb_contents+1)/2-nb_contents));
		System.out.println("OVerall average VSM BOW similarity : "+total_sum/(nb_contents*(nb_contents+1)/2-nb_contents));

		System.out.println("Total exact sum : "+total_exact_number);
		System.out.println("Total number of comparisons : "+(nb_contents*(nb_contents+1)/2-nb_contents));
		System.out.println("OVerall average VSM BOW similarity : "+total_exact_number/(nb_contents*(nb_contents+1)/2-nb_contents));

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
