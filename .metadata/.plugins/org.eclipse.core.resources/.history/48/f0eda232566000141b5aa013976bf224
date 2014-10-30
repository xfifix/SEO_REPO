package com.multirequesting;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class ExaleadRequestingWorkerThread implements Runnable {
	private String user_agent;
	private String description;
	private List<Integer> thread_fetch_ids = new ArrayList<Integer>();
	private List<String> my_urls_to_fetch = new ArrayList<String>();
	private Connection con;


	public ExaleadRequestingWorkerThread(Connection con ,List<Integer> to_fetch, String my_user_agent, String my_description) throws SQLException{
		this.user_agent=my_user_agent;
		this.description=my_description;
		this.thread_fetch_ids=to_fetch;
		this.con = con;
		PreparedStatement pst  = null;
		if ("".equals(my_description)){
			String my_url="SELECT URL FROM HTTPINFOS_LIST WHERE TO_FETCH = TRUE and ID in "+to_fetch.toString();
			my_url=my_url.replace("[", "(");
			my_url=my_url.replace("]", ")");
			pst = con.prepareStatement(my_url);
		} else {
			String my_url="SELECT URL FROM HTTPINFOS_LIST WHERE TO_FETCH = TRUE and DESCRIPTION='"+description+"' and ID in "+to_fetch.toString();
			my_url=my_url.replace("[", "(");
			my_url=my_url.replace("]", ")");
			pst = con.prepareStatement(my_url);
		};
		ResultSet rs = null;
		rs = pst.executeQuery();
		while (rs.next()) {
			my_urls_to_fetch.add(rs.getString(1)); 
		}
	}

	public void run() {
		List<URLInfo> infos=processCommand();

		try {
			updateStatus(infos);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println(Thread.currentThread().getName()+" End");
	}

	// batched update
		private void updateStatus(List<URLInfo> infos) throws SQLException{
			Statement st = con.createStatement();       
			con.setAutoCommit(false);      
			for (int i=0;i<infos.size();i++){
				String H1= infos.get(i).getH1().replace("'", "");
				String TITLE = infos.get(i).getTitle().replace("'", "");
				String batch ="UPDATE HTTPINFOS_LIST SET STATUS="+infos.get(i).getStatus()+", H1='"+H1+"', TITLE='"+TITLE+ "',TO_FETCH=FALSE WHERE ID="+thread_fetch_ids.get(i);
				st.addBatch(batch);
			}      
			//int counts[] = st.executeBatch();
			st.executeBatch();
			con.commit();
			System.out.println("Inserting : " + infos.size() + "ULRs into database");
		}

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

	private List<URLInfo> processCommand() {
		List<URLInfo> my_fetched_infos = new ArrayList<URLInfo>();
		for (int i=0;i<my_urls_to_fetch.size();i++){
			String line=my_urls_to_fetch.get(i);
			// second method
			URLInfo my_info = new URLInfo();
			HttpURLConnection connection = null;
			try{
				System.out.println(Thread.currentThread().getName()+" fetching URL : "+line);
				URL url = new URL(line);
				connection = (HttpURLConnection)url.openConnection();
				connection.setRequestMethod("GET");
				connection.setRequestProperty("User-Agent",this.user_agent);
				connection.setInstanceFollowRedirects(true);
				connection.setConnectTimeout(30000);
				connection.connect();
				// getting the status from the connection
				my_info.setStatus(connection.getResponseCode());
				// getting the content to parse
				InputStreamReader in = new InputStreamReader((InputStream) connection.getContent());
				BufferedReader buff = new BufferedReader(in);
				String content_line;
				StringBuilder builder=new StringBuilder();
				do {
					content_line = buff.readLine();
					builder.append(content_line);
				} while (content_line != null);
				String html = builder.toString();
				org.jsoup.nodes.Document doc = Jsoup.parse(html);
				Elements h1s = doc.select("h1");
				String conc_h1="";
				for (Element h1 : h1s) {
					conc_h1=conc_h1+h1.text();
				}	
				my_info.setH1(conc_h1);
				Elements titles = doc.select("title");
				String conc_title="";
				for (Element title : titles) {
					conc_title=conc_title+title.text();
				}				
				my_info.setTitle(conc_title);
			} catch (Exception e){
				System.out.println("Error with "+line);
				e.printStackTrace();
			}

			if (connection != null){
				connection.disconnect();
			}
			my_fetched_infos.add(my_info);
		}
		return my_fetched_infos;
	}


	class URLInfo{
		private String h1="";
		private String title="";
		private int status=-1;
		public String getH1() {
			return h1;
		}
		public void setH1(String h1) {
			this.h1 = h1;
		}
		public String getTitle() {
			return title;
		}
		public void setTitle(String title) {
			this.title = title;
		}
		public int getStatus() {
			return status;
		}
		public void setStatus(int status) {
			this.status = status;
		}
	}

}
