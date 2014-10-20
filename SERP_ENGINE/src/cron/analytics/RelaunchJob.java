package cron.analytics;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class RelaunchJob {
	public static void main(String[] args){
		if(args.length==0){
			System.out.println("You must specify the idRun you want to relaunch");
		}
		try{
			String idRun = args[0];
			DataBaseManagement.instantiante_connection();
			DataBaseManagement.reopen(idRun);
			String idCheck = DataBaseManagement.get_check_id(idRun);
			// getting the target associated with the group
			//			ResultSet target_resultSet = DataBaseManagement.search_target(idGroup);
			//			while (target_resultSet.next()) {
			//			target=target_resultSet.getString("name");
			//			idTarget=target_resultSet.getString("idTarget");
			//			System.out.println("target: " + target);
			//			System.out.println("idTarget: " + idTarget);
			//		}		

			String target = "wwww.cdiscount.com";
			String idTarget = "1";
			//
			ResultSet missing_keyword_resultSet = DataBaseManagement.search_missing_keywords(idCheck);
			while (missing_keyword_resultSet.next()) {
				String idKeyword = missing_keyword_resultSet.getString("idKeyword");			
				String keyword_name = missing_keyword_resultSet.getString("name");
				//System.out.println("idKeyword: " + idKeyword);
				System.out.println("Launching keyword: " + keyword_name);

				// asynchronous launch
				//GoogleSearchSaveTask beep=new GoogleSearchSaveTask(checkId, idTarget,  idKeyword,keyword_name);

				// Synchronous launch but waiting after
				RankInfo loc_info = ranking_keyword(keyword_name,target);
				DataBaseManagement.insertKeyword(idCheck, idTarget,  idKeyword,loc_info.getPosition(), loc_info.getUrl()); 
			}

			// closing the run by inserting a stopping date !
			DataBaseManagement.close_current_run();
		} catch (SQLException e){
			e.printStackTrace();
		} finally{
			DataBaseManagement.close();
		}
	}
	public static RankInfo ranking_keyword(String keyword, String targe_name){
		RankInfo info= new RankInfo();
		info.setKeyword(keyword);
		// we here fetch up to five paginations
		int nb_depth = 5;
		long startTimeMs = System.currentTimeMillis( );
		org.jsoup.nodes.Document doc;
		int depth=0;
		int nb_results=0;
		int my_rank=50;
		String my_url = "";
		boolean found = false;
		while (depth<nb_depth && !found){
			try{
				// we wait between 30 and 70 seconds
				Thread.sleep(randInt(30,50)*1000);
				System.out.println("Fetching a new page");
				doc =  Jsoup.connect(
						"https://www.google.fr/search?q="+keyword+"&start="+Integer.toString(depth*10))
						.userAgent("Mozilla/5.0 (Windows; U; Windows NT 6.1; en-GB;     rv:1.9.2.13) Gecko/20101203 Firefox/3.6.13 (.NET CLR 3.5.30729)")
						.referrer("accounterlive.com")
						.ignoreHttpErrors(true)
						.timeout(0)
						.get();
				Elements serps = doc.select("h3[class=r]");
				for (Element serp : serps) {
					Element link = serp.getElementsByTag("a").first();
					String linkref = link.attr("href");
					if (linkref.startsWith("/url?q=")){
						nb_results++;
						linkref = linkref.substring(7,linkref.indexOf("&"));
					}
					if (linkref.contains(targe_name)){
						my_rank=nb_results;
						my_url=linkref;
						found=true;
					}			
					//					System.out.println("Link ref: "+linkref);
					//					System.out.println("Title: "+serp.text());
				}
				if (nb_results == 0){
					System.out.println("Warning captcha");
				}
				depth++;
			}
			catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		long taskTimeMs  = System.currentTimeMillis( ) - startTimeMs;
		//System.out.println(taskTimeMs);
		info.setPosition(my_rank);
		info.setUrl(my_url);
		if (nb_results == 0){
			System.out.println("Warning captcha");
		}else {
			System.out.println("Number of links : "+nb_results);
		}
		System.out.println("My rank : "+my_rank+" for keyword : "+keyword);
		System.out.println("My URL : "+my_url+" for keyword : "+keyword);
		return info;
	}

	public static int randInt(int min, int max) {
		// NOTE: Usually this should be a field rather than a method
		// variable so that it is not re-seeded every call.
		Random rand = new Random();
		// nextInt is normally exclusive of the top value,
		// so add 1 to make it inclusive
		int randomNum = rand.nextInt((max - min) + 1) + min;
		return randomNum;
	}
}


