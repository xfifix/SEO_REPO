package com.facettes.update;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class URLListFacettesUpdatingWorkerThread implements Runnable {

	private static Pattern bracketPattern = Pattern.compile("\\(.*?\\)");
	private int batch_size = 100;

	private static String select_statement = "SELECT URL, ID FROM FACETTES_LIST WHERE TO_FETCH = TRUE and ID in ";
	private static String insertStatement ="INSERT INTO FACETTES_LIST_RESULTS (URL,FACETTE_NAME,FACETTE_VALUE,FACETTE_COUNT) VALUES(?, ?, ?, ?)";
	private static String updateStatement ="UPDATE FACETTES_LIST SET TO_FETCH=FALSE WHERE ID=";

	private String user_agent;
	private List<ULRId> my_urls_to_fetch = new ArrayList<ULRId>();
	private Connection con;

	public URLListFacettesUpdatingWorkerThread(Connection con ,List<Integer> to_fetch, String my_user_agent) throws SQLException{
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
		runBatch(line_infos);
		close_connection();
		System.out.println(Thread.currentThread().getName()+" closed connection");
	}

	public void runBatch(List<ULRId> line_infos){
		List<FacettesInfo> infos=processCommand(line_infos);
		updateResults(infos);
		updateStatus(infos);
		
		//updateStatusStepByStep(infos);
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

	private void updateResults(List<FacettesInfo> infos){
		System.out.println("Adding to batch : " + infos.size() + "ULRs into database");
		try {
			//Statement st = con.createStatement();
			con.setAutoCommit(false); 
			PreparedStatement st = con.prepareStatement(insertStatement);
			for (FacettesInfo info_to_update : infos){
				String url_to_update = info_to_update.getUrl();
				String facette_name = info_to_update.getFacetteName();
				String facette_value = info_to_update.getFacetteValue();
				int facette_count = info_to_update.getFacetteCount();
				st.setString(1, url_to_update);
				st.setString(2, facette_name);
				st.setString(3, facette_value);
				st.setInt(4, facette_count);
				st.addBatch();		
			}      
			//int counts[] = st.executeBatch();
			System.out.println("Beginning to insert : " + infos.size() + "ULRs into database");
			st.executeBatch();
			con.commit();
			st.close();
			System.out.println("Having inserted : " + infos.size() + "ULRs into database");
		} catch (SQLException e){
			e.printStackTrace();
			System.out.println("Trouble inserting batch ");
		}
	}

	
	// batched update
	private void updateStatus(List<FacettesInfo> infos){
		try{
		Statement st = con.createStatement();       
		con.setAutoCommit(false);      
		for (int i=0;i<infos.size();i++){
			String batch =updateStatement+infos.get(i).getId();
			st.addBatch(batch);
		}      
		//int counts[] = st.executeBatch();
		st.executeBatch();
		con.commit();
		System.out.println("Inserting : " + infos.size() + "ULRs into database");
		} catch (SQLException e){
			e.printStackTrace();
			System.out.println("Trouble inserting batch ");
		}
	}

	// update step by step
	private void updateResultsStepByStep(List<FacettesInfo> infos){
		System.out.println("Adding to batch : " + infos.size() + "ULRs into database");
		try {
			//Statement st = con.createStatement();
			PreparedStatement st = con.prepareStatement(insertStatement);
			for (FacettesInfo info_to_update : infos){
				String url_to_update = info_to_update.getUrl();
				String facette_name = info_to_update.getFacetteName();
				String facette_value = info_to_update.getFacetteValue();
				int facette_count = info_to_update.getFacetteCount();
				st.setString(1, url_to_update);
				st.setString(2, facette_name);
				st.setString(3, facette_value);
				st.setInt(4, facette_count);
				st.executeUpdate();		
			}      
			//int counts[] = st.executeBatch();
			System.out.println("Beginning to insert : " + infos.size() + "ULRs into database");
			st.close();
			System.out.println("Having inserted : " + infos.size() + "ULRs into database");
		} catch (SQLException e){
			e.printStackTrace();
			System.out.println("Trouble inserting batch ");
		}
	}

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
				my_info.setUrl(url);

				Elements facette_elements = doc.select("div.mvFacets.jsFCategory.mvFOpen");			
				for (Element facette : facette_elements ){
					//System.out.println(e.toString());
					Elements facette_name = facette.select("div.mvFTitle.noSel");
					my_info.setFacetteName(facette_name.text());
					Elements facette_values = facette.select("a");
					for (Element facette_value : facette_values){		
						System.out.println(facette_value);
						// old way
						String categorie_value = facette_value.text();
						if ("".equals(categorie_value)){
							categorie_value = facette_value.attr("title");
						}
						Matcher matchPattern = bracketPattern.matcher(categorie_value);
						String categorieCount ="";
						while (matchPattern.find()) {		
							categorieCount=matchPattern.group();
						}
						categorie_value=categorie_value.replace(categorieCount,"");
						categorieCount=categorieCount.replace("(", "");
						categorieCount=categorieCount.replace(")", "");	
						//System.out.println(categorie_value);
						try{
							my_info.setFacetteCount(Integer.valueOf(categorieCount));
							//System.out.println(Integer.valueOf(categorieCount));	
						} catch (NumberFormatException e){
							System.out.println("Trouble while formatting a facette");
							my_info.setFacetteCount(0);
						}
						my_info.setFacetteValue(categorie_value);
						my_fetched_infos.add(my_info);
						my_info = new FacettesInfo();
						my_info.setFacetteName(facette_name.text());
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
		private String url;
		private String facetteName;
		private String facetteValue;
		private int facetteCount;
		public String getUrl() {
			return url;
		}
		public void setUrl(String url) {
			this.url = url;
		}
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
		public int getFacetteCount() {
			return facetteCount;
		}
		public void setFacetteCount(int facetteCount) {
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
