package com.facettes.fromscratch;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.facettes.data.AdvancedFacettesInfo;


public class URLListFacettesWorkerThread implements Runnable {

	private static Pattern bracketPattern = Pattern.compile("\\(.*?\\)");
	private int batch_size = 100;

	private static String select_statement = "SELECT URL, ID FROM FACETTES_LIST WHERE TO_FETCH = TRUE and ID in ";
	private static String insertStatement ="INSERT INTO FACETTES_LIST_RESULTS (URL,FACETTE_NAME,FACETTE_VALUE,FACETTE_COUNT, PRODUCT_COUNT, IS_OPEN, OPENED_FACETTE_URL) VALUES(?, ?, ?, ?, ?, ?, ?)";
		
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
		runBatch(line_infos);
		close_connection();
		System.out.println(Thread.currentThread().getName()+" closed connection");
	}

	public void runBatch(List<ULRId> line_infos){
		List<AdvancedFacettesInfo> infos=processCommand(line_infos);
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

	// batched update
	private void updateStatus(List<AdvancedFacettesInfo> infos){
		System.out.println("Adding to batch : " + infos.size() + "ULRs into database");
		try {
			//Statement st = con.createStatement();
			con.setAutoCommit(false); 
			PreparedStatement st = con.prepareStatement(insertStatement);
			for (AdvancedFacettesInfo info_to_update : infos){
				String url_to_update = info_to_update.getUrl();
				String facette_name = info_to_update.getFacetteName();
				String facette_value = info_to_update.getFacetteValue();
				int facette_count = info_to_update.getFacetteCount();
				String products_size = info_to_update.getProducts_size();
				boolean isopened = info_to_update.isIs_opened();
				String lfURL = info_to_update.getOpened_facette_url();
				st.setString(1, url_to_update);
				st.setString(2, facette_name);
				st.setString(3, facette_value);
				st.setInt(4, facette_count);
				st.setString(5, products_size);
				st.setBoolean(6, isopened);
				st.setString(7, lfURL);				
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

	// update step by step
	private void updateStatusStepByStep(List<AdvancedFacettesInfo> infos){
		System.out.println("Adding to batch : " + infos.size() + "ULRs into database");
		try {
			//Statement st = con.createStatement();
			PreparedStatement st = con.prepareStatement(insertStatement);
			for (AdvancedFacettesInfo info_to_update : infos){
				String url_to_update = info_to_update.getUrl();
				String facette_name = info_to_update.getFacetteName();
				String facette_value = info_to_update.getFacetteValue();
				int facette_count = info_to_update.getFacetteCount();
				String products_size = info_to_update.getProducts_size();
				boolean isopened = info_to_update.isIs_opened();
				String lfURL = info_to_update.getOpened_facette_url();
				st.setString(1, url_to_update);
				st.setString(2, facette_name);
				st.setString(3, facette_value);
				st.setInt(4, facette_count);
				st.setString(5, products_size);
				st.setBoolean(6, isopened);
				st.setString(7, lfURL);		
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

	private List<AdvancedFacettesInfo> processCommand(List<ULRId> line_infos) {
		List<AdvancedFacettesInfo> my_fetched_infos = new ArrayList<AdvancedFacettesInfo>();
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
				AdvancedFacettesInfo my_info = new AdvancedFacettesInfo();
				my_info.setId(idUrl);
				my_info.setUrl(url);

				Elements counter_elements = doc.select(".lpStTit strong");		
				String product_size_text = counter_elements.text();
				my_info.setProducts_size(product_size_text);
				boolean isFacetteOpened = false;
				
				Elements facette_elements = doc.select("div.mvFacets.jsFCategory.mvFOpen");			
				for (Element facette : facette_elements ){
					//System.out.println(e.toString());
					Elements facette_name = facette.select("div.mvFTitle.noSel");
					my_info.setFacetteName(facette_name.text());
					Elements facette_values = facette.select("a");
					for (Element facette_value : facette_values){		
						//System.out.println(facette_value);
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
						my_info.setIs_opened(isFacetteOpened);
						my_fetched_infos.add(my_info);
						my_info = new AdvancedFacettesInfo();
						my_info.setId(idUrl);
						my_info.setUrl(url);
						my_info.setProducts_size(product_size_text);
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


}
