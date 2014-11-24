package crawl4j.vsm;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class InsideMagasinBetweenRayonWorkerThread implements Runnable {
	private static String insert_statement="INSERT INTO RAYON_SIMILARITIES (MAGASIN, RAYON1, RAYON2, PERCENTAGE_EXACT, AVG_SIMILARITY, SIMILARITIES, LAST_UPDATE) VALUES (?,?,?,?,?,?,?)";
	private String magasin;
	private Map<String,List<URLContentInfo>> rayons_datas = new HashMap<String,List<URLContentInfo>>();
	private List<String> rayons;
	private String field_to_fetch;
	private Connection con;
	private double threshold = 0.8;

	public InsideMagasinBetweenRayonWorkerThread(Connection con, String magasin,List<String> rayons, String field_to_fetch) throws SQLException{
		this.magasin=magasin;
		this.rayons=rayons;
		this.field_to_fetch=field_to_fetch;
		System.out.println("Computing metrics for field : "+ this.field_to_fetch +" and for "+magasin +" :"+ this.magasin);
		this.con = con;
		for (String rayon : rayons){
			System.out.println("Getting content from rayon :"+rayon);
			List<URLContentInfo> info_list = rayons_datas.get(rayon);
			if (info_list==null){
				info_list=new ArrayList<URLContentInfo>();
				rayons_datas.put(rayon,info_list);
			}	
			// getting the URLs infos for each rayon
			PreparedStatement field_pst;
			try {
				field_pst  = this.con.prepareStatement("SELECT "+this.field_to_fetch+" , URL, VENDOR, MAGASIN, RAYON, PRODUIT FROM CRAWL_RESULTS WHERE RAYON='" +rayon+ "'");
				ResultSet field_rs = field_pst.executeQuery();
				while (field_rs.next()) {
					URLContentInfo url_info = new URLContentInfo();
					String content = field_rs.getString(1);
					String my_url = field_rs.getString(2);
					String my_vendor = field_rs.getString(3);
					String my_magasin = field_rs.getString(4);
					String my_rayon = field_rs.getString(5);
					String my_produit = field_rs.getString(6);
					url_info.setContent(content);
					url_info.setUrl(my_url);
					url_info.setMagasin(my_magasin);
					url_info.setRayon(my_rayon);
					url_info.setProduit(my_produit);
					url_info.setVendor(my_vendor);
					info_list.add(url_info);
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				System.out.println("Database trouble with the rayon :"+rayon);
				e.printStackTrace();
			}
		}
	}

	public void run() {
		for (String rayon1 : rayons){
			for (String rayon2 : rayons){
				System.out.println("Computing similarity metrics for rayons : "+ rayon1 + " and "+ rayon2);
				RayonComparison tosave = buildComparisonMatrix(rayon1,rayon2);
				save_database(tosave);
			}	
		}
		System.out.println(Thread.currentThread().getName()+" End");
	}

	private RayonComparison buildComparisonMatrix(String rayoni, String rayonj){
		System.out.println("Computing similarity metrics between rayons : "+rayoni+"/"+rayonj);
		List<URLContentInfo> info_i = rayons_datas.get(rayoni);
		List<URLContentInfo> info_j = rayons_datas.get(rayonj);
		RayonComparison comp =cross_lists(info_i,info_j);
		comp.setRayoni(rayoni);
		comp.setRayonj(rayonj);		
		System.out.println("First rayon size : " + comp.getRayon_i_size());
		System.out.println("Second rayon size : " + comp.getRayon_j_size());
		System.out.println("Percentage of exactly matching : " +comp.getPercent_exactly_matching());
		System.out.println("Average similarity : " + comp.getAverage_similarity());
		return comp;
	}
	private RayonComparison cross_lists(List<URLContentInfo> infos_i,List<URLContentInfo> infos_j){
		int total_exact_number=0;
		double total_sum = 0;
		int rayon_i_size = infos_i.size();
		int rayon_j_size = infos_j.size();
		List<RayonLevelDoublon> inbetweendoublons = new ArrayList<RayonLevelDoublon>();
		for (int k=0;k<rayon_i_size;k++){
			URLContentInfo infok=infos_i.get(k);
			String urlk=infok.getUrl();
			int indexk = urlk.indexOf("?");
			if (indexk>=0){
				urlk=urlk.substring(0, indexk);
			}
			String contentk =infok.getContent() == null ? "" : infok.getContent();
			for (int l=0;l<rayon_j_size;l++){
				URLContentInfo infol=infos_j.get(l);
				String urll= infol.getUrl();
				int indexl = urll.indexOf("?");
				if (indexl>=0){
					urll=urll.substring(0, indexl);
				}
				String contentl =infol.getContent() == null ? "" : infol.getContent();
				if (!(urll.equals(urlk))){
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
						RayonLevelDoublon doublon = new RayonLevelDoublon();
						doublon.setURL1(infok.getUrl());
						doublon.setURL2(infol.getUrl());
						doublon.setIsexact(true);
						doublon.setSimilarity(similarity);
						inbetweendoublons.add(doublon);
					}
					total_sum=total_sum+similarity;
					if(similarity>=threshold && !isexactlysimilar){
						RayonLevelDoublon doublon = new RayonLevelDoublon();
						doublon.setURL1(infok.getUrl());
						doublon.setURL2(infol.getUrl());
						doublon.setIsexact(false);
						doublon.setSimilarity(similarity);
						inbetweendoublons.add(doublon);
					}
				}
			}
		}
		RayonComparison comp = new RayonComparison();
		comp.setRayon_i_size(rayon_i_size);
		comp.setRayon_j_size(rayon_j_size);
		comp.setPercent_exactly_matching( ((double)total_exact_number)/(rayon_i_size*rayon_j_size));
		comp.setAverage_similarity((total_sum)/(rayon_i_size*rayon_j_size));
		comp.setDoublons(inbetweendoublons);
		return comp;
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


	private void save_database(RayonComparison comp){
		String urls_to_insert = "";
		try {
			PreparedStatement ps = con.prepareStatement(insert_statement);
			ps.setString(1, this.magasin);
			ps.setString(2, comp.getRayoni());
			ps.setString(3, comp.getRayonj());
			ps.setDouble(4, comp.getPercent_exactly_matching());
			ps.setDouble(5, comp.getAverage_similarity());
			urls_to_insert = getFileDoublons(comp);
			ps.setString(6,urls_to_insert);
			//urls.toString().getBytes(StandardCharsets.UTF_8);
			//ps.setBinaryStream(4,new ByteArrayInputStream(urls_to_print), urls_to_print.length);
			java.sql.Date sqlDate = new java.sql.Date(System.currentTimeMillis());
			ps.setDate(7,sqlDate);
			ps.executeUpdate();
		} catch (SQLException e) {
			System.out.println("Trouble inserting "+comp.getRayoni()+" "+comp.getRayonj());
			System.out.println(urls_to_insert);
			e.printStackTrace();
		}
	}

	private String getFileDoublons(RayonComparison comp){
		List<RayonLevelDoublon>  doublon_list = comp.getDoublons();
		StringBuilder urls = new StringBuilder();
		for (RayonLevelDoublon doublon : doublon_list){
			urls.append(doublon.getURL1()+";"+doublon.getURL2()+"\n");
		}
		return  urls.toString();
	}
}
