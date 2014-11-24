package crawl4j.vsm;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class InsideMagasinSimilarityWorkerThread implements Runnable {
	private static String insert_statement="INSERT INTO MAGASIN_SIMILARITIES (MAGASIN, CONTENT, RAYONS, EXACT_COUNT, COUPLES, LAST_UPDATE) VALUES (?,?,?,?,?,?)";
	private String magasin;
	private Map<String,List<URLContentInfo>> magasins_duplicates = new HashMap<String,List<URLContentInfo>>();
	private List<URLContentInfo> magasin_infos = new ArrayList<URLContentInfo>();
	private String field_to_fetch;
	private Connection con;

	public InsideMagasinSimilarityWorkerThread(Connection con, String magasin, String field_to_fetch) throws SQLException{
		this.magasin=magasin;
		//this.rayons=rayons;
		this.field_to_fetch=field_to_fetch;
		System.out.println("Computing metrics for field : "+ this.field_to_fetch +" and for "+magasin +" :"+ this.magasin);
		this.con = con;

		System.out.println("Getting content from magasin :"+this.magasin);
		PreparedStatement field_pst;
		try {
			field_pst  = this.con.prepareStatement("SELECT "+this.field_to_fetch+" , URL, VENDOR, MAGASIN, RAYON, PRODUIT FROM CRAWL_RESULTS WHERE MAGASIN='" +magasin+ "'");
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
				magasin_infos.add(url_info);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println("Database trouble with the magasin :"+this.magasin);
			e.printStackTrace();
		}
	}

	public void run() {
		for (URLContentInfo info : magasin_infos){
			String content = info.getContent();
			List<URLContentInfo> content_aggregation = magasins_duplicates.get(content);
			if (content_aggregation == null){
				content_aggregation=new ArrayList<URLContentInfo>();
				magasins_duplicates.put(content, content_aggregation);
			}
			content_aggregation.add(info);			
		}
		save_results_database();
	}


	private void save_results_database(){
		// we here loop over all our fields
		System.out.println("Displaying arguments counting results \n");
		Iterator it = magasins_duplicates.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pairs = (Map.Entry)it.next();
			String content=(String)pairs.getKey();
			List<URLContentInfo> content_infos = (List<URLContentInfo>)pairs.getValue();
			int nb_exact_urls = content_infos.size();
			if ((!(content == null))&& nb_exact_urls > 1){ 
				Set<String> rayon_set = new HashSet<String>();
				Set<String> url_set = new HashSet<String>();
				for (URLContentInfo content_info:content_infos){
					url_set.add(content_info.getUrl());
					rayon_set.add(content_info.getRayon());
				}
				StringBuilder duplicates_aggregator = new StringBuilder();
				for (String local_url : url_set) {
					duplicates_aggregator.append(local_url+";");
				}
				String duplicates_urls = duplicates_aggregator.toString();
				StringBuilder rayons_aggregator = new StringBuilder();
				for (String rayon : rayon_set) {
					rayons_aggregator.append(rayon+";");
				}
				String all_rayons =rayons_aggregator.toString();
				System.out.println("Inserting : "+nb_exact_urls+" : "+content);
				try {
					PreparedStatement ps = con.prepareStatement(insert_statement);
					ps.setString(1, this.magasin);
					ps.setString(2, content);
					ps.setString(3, all_rayons);
					ps.setInt(4, nb_exact_urls);
					ps.setString(5, duplicates_urls);
					java.sql.Date sqlDate = new java.sql.Date(System.currentTimeMillis());
					ps.setDate(6,sqlDate);
					ps.executeUpdate();
				} catch (SQLException e) {
					System.out.println("Trouble inserting "+this.magasin);
					e.printStackTrace();
				}
			}
		}	
	}
}
