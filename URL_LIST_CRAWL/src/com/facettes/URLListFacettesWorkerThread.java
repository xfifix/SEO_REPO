package com.facettes;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class URLListFacettesWorkerThread implements Runnable {
	private int batch_size = 100;
	private static String select_statement = "SELECT URL, ID FROM FACETTES_LIST WHERE TO_FETCH = TRUE and ID in ";
	private static String updateStatement ="UPDATE HTTPINFOS_LIST SET STATUS=?, H1=?, TITLE=?, XPATH1=?, XPATH2=?, XPATH3=?, XPATH4=?, XPATH5=?, TO_FETCH=FALSE WHERE ID=?";
	private String user_agent;
	private List<ULRId> my_urls_to_fetch = new ArrayList<ULRId>();
	private Connection con;

	public URLListFacettesWorkerThread(Connection con ,List<Integer> to_fetch, String my_user_agent) throws SQLException{
		this.user_agent=my_user_agent;
		this.con = con;
		String my_url="";
		if (to_fetch.size()>0){
			try {
				PreparedStatement pst = null;
				my_url=select_statement+to_fetch.toString();
				my_url=my_url.replace("[", "(");
				my_url=my_url.replace("]", ")");
				pst = con.prepareStatement(my_url);
				ResultSet rs = null;
				rs = pst.executeQuery();
				while (rs.next()) {
					String loc_url = rs.getString(1);
					int id = rs.getInt(2);
					ULRId toadd = new ULRId();
					toadd.setId(id);
					toadd.setUrl(loc_url);
					my_urls_to_fetch.add(toadd); 
				}
				pst.close();
				System.out.println(Thread.currentThread()+" initialized with  : "+to_fetch.size() + " fetched URLs");

			}
			catch(SQLException e){
				e.printStackTrace();
				System.out.println("Trouble with thread"+Thread.currentThread()+" and URL : "+my_url);
			}
		}
	}

	public void run() {
		List<ULRId> line_infos = new ArrayList<ULRId>();
		for (ULRId id :my_urls_to_fetch){
			line_infos.add(id);
			if (line_infos.size() !=0 && line_infos.size() % batch_size ==0) {
				runBatch(line_infos);	
				line_infos.clear();
				line_infos = new ArrayList<ULRId>();
			}
		}
		close_connection();
		System.out.println(Thread.currentThread().getName()+" closed connection");
	}

	public void runBatch(List<ULRId> line_infos){
		List<FacettesInfo> infos=processCommand(line_infos);
		//updateStatus(infos);
		System.out.println(Thread.currentThread().getName()+" End");
	}

	private void close_connection(){
		try {
			this.con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// batched update
//	private void updateStatus(List<URLInfo> infos){
//		System.out.println("Adding to batch : " + infos.size() + "ULRs into database");
//		try {
//			//Statement st = con.createStatement();
//			con.setAutoCommit(false); 
//			PreparedStatement st = con.prepareStatement(updateStatement);
//			for (int i=0;i<infos.size();i++){
//				String H1= infos.get(i).getH1().replace("'", "");
//				String TITLE = infos.get(i).getTitle().replace("'", "");
//				String[] XPATHRESULTS = infos.get(i).getXpathResults();
//				st.setInt(1, infos.get(i).getStatus());
//				st.setString(2, H1);
//				st.setString(3, TITLE);
//				if (XPATHRESULTS != null){
//					st.setString(4, XPATHRESULTS[0]);
//					st.setString(5, XPATHRESULTS[1]);
//					st.setString(6, XPATHRESULTS[2]);
//					st.setString(7, XPATHRESULTS[3]);
//					st.setString(8, XPATHRESULTS[4]);
//				}else {
//					st.setString(4, "");
//					st.setString(5, "");
//					st.setString(6, "");
//					st.setString(7, "");
//					st.setString(8, "");
//				}
//				st.setInt(9, infos.get(i).getId());
//				//UPDATE HTTPINFOS_LIST SET STATUS=?, H1=?, TITLE=?, XPATH1=?, XPATH2=?, XPATH3=?, XPATH4=?, XPATH5=?, TO_FETCH=FALSE WHERE ID=?";
//				//	String batch ="UPDATE HTTPINFOS_LIST SET STATUS="+infos.get(i).getStatus()+", H1='"+H1+"', TITLE='"+TITLE+ "',TO_FETCH=FALSE WHERE ID="+infos.get(i).getId();
//				st.addBatch();		
//			}      
//			//int counts[] = st.executeBatch();
//			System.out.println("Beginning to insert : " + infos.size() + "ULRs into database");
//			st.executeBatch();
//			con.commit();
//			st.close();
//			System.out.println("Having inserted : " + infos.size() + "ULRs into database");
//		} catch (SQLException e){
//			e.printStackTrace();
//			System.out.println("Trouble inserting batch ");
//		}
//	}

	// update step by step
	//	private void updateStatus(List<URLInfo> infos){
	//		for (int i=0;i<infos.size();i++){
	//			String H1= infos.get(i).getH1().replace("'", "");
	//			String TITLE = infos.get(i).getTitle().replace("'", "");
	//			String batch ="UPDATE HTTPINFOS_LIST SET STATUS="+infos.get(i).getStatus()+", H1='"+H1+"', TITLE='"+TITLE+ "',TO_FETCH=FALSE WHERE ID="+thread_fetch_ids.get(i);
	//			try{
	//				PreparedStatement insert_st = con.prepareStatement(batch);
	//				insert_st.executeUpdate();
	//			} catch (SQLException e){
	//				System.out.println("Trouble inserting : "+batch);
	//				e.printStackTrace();
	//			}
	//
	//		}      
	//		System.out.println("Inserting : " + infos.size() + "ULRs into database");
	//	}

	private List<FacettesInfo> processCommand(List<ULRId> line_infos) {
		List<FacettesInfo> my_fetched_infos = new ArrayList<FacettesInfo>();
		for(ULRId line_info : line_infos){
			int idUrl = line_info.getId();
			String url = line_info.getUrl();

			System.out.println(Thread.currentThread().getName()+" fetching URL : "+url);
			// fetching the URL and parsing the results
			org.jsoup.nodes.Document doc;

			try {
				doc =  Jsoup.connect(url)
						.userAgent(user_agent)
						.ignoreHttpErrors(true)
						.timeout(0)
						.get();
				FacettesInfo my_info = new FacettesInfo();
				my_info.setId(idUrl);
				Elements facette_elements = doc.select("div.mvFilter");			
				for (Element facette : facette_elements ){
					//System.out.println(e.toString());
					Elements facette_name = facette.select("div.mvFTit");
					my_info.setFacetteName(facette_name.text());
		
					Elements facette_values = facette.select("a");
					for (Element facette_value : facette_values){
						my_info.setFacetteValue(facette_value.text());
						my_fetched_infos.add(my_info);
						my_info = new FacettesInfo();
						my_info.setId(idUrl);
					}		
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		return my_fetched_infos;
	}

	class ULRId{
		private String url="";
		private int id;
		public String getUrl() {
			return url;
		}
		public void setUrl(String url) {
			this.url = url;
		}
		public int getId() {
			return id;
		}
		public void setId(int id) {
			this.id = id;
		}
	}


	class FacettesInfo{
		private int id;
		private String facetteName;
		private String facetteValue;
		private String facetteCount;
		public String getFacetteName() {
			return facetteName;
		}
		public void setFacetteName(String facetteName) {
			this.facetteName = facetteName;
		}
		public String getFacetteValue() {
			return facetteValue;
		}
		public void setFacetteValue(String facetteValue) {
			this.facetteValue = facetteValue;
		}
		public String getFacetteCount() {
			return facetteCount;
		}
		public void setFacetteCount(String facetteCount) {
			this.facetteCount = facetteCount;
		}
		public int getId() {
			return id;
		}
		public void setId(int id) {
			this.id = id;
		}
	}

}
