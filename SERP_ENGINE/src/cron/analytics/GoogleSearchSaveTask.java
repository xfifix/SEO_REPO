package cron.analytics;

import java.sql.SQLException;
import java.util.Timer;
import java.util.TimerTask;

public class GoogleSearchSaveTask {
	Timer timer;

	private static int nb_request=0;
	private static int delayed_seconds = 30;

	public GoogleSearchSaveTask(String checkId, String idTarget, String idKeyword,String keyword) {
		nb_request++;
		timer = new Timer();  //At this line a new Thread will be created
		RankingTask task = new RankingTask();
		task.setKeyword_to_search(keyword);
		task.setIdCheck(checkId);
		task.setIdKeyword(idKeyword);
		task.setIdTarget(idTarget);
		//timer.schedule(task, nb_request*delayed_seconds*1000); //delay in milliseconds
		timer.schedule(task, nb_request*delayed_seconds); //delay in milliseconds
	}

	class RankingTask extends TimerTask {
		private String keyword_to_search;
		private String idCheck;
		private String idTarget;
		private String idKeyword;

		@Override
		public void run() {
			System.out.println("ReminderTask is completed by Java timer :"+keyword_to_search+idCheck+idTarget+idKeyword);
			timer.cancel(); //Not necessary because we call System.exit
			//System.exit(0); //Stops the AWT thread (and everything else)
			RankInfo loc_info = ranking_keyword(keyword_to_search);
			try {
				DataBaseManagement.insertKeyword(idCheck, idTarget,  idKeyword,loc_info.getPosition(), loc_info.getUrl());
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println("Error when database saving " + keyword_to_search);
			} 
			
		}
		public String getIdCheck() {
			return idCheck;
		}

		public void setIdCheck(String idCheck) {
			this.idCheck = idCheck;
		}

		public String getIdTarget() {
			return idTarget;
		}

		public void setIdTarget(String idTarget) {
			this.idTarget = idTarget;
		}

		public String getIdKeyword() {
			return idKeyword;
		}

		public void setIdKeyword(String idKeyword) {
			this.idKeyword = idKeyword;
		}
		public String getKeyword_to_search() {
			return keyword_to_search;
		}

		public void setKeyword_to_search(String keyword_to_search) {
			this.keyword_to_search = keyword_to_search;
		}

	}

	public RankInfo ranking_keyword(String keyword){
		RankInfo info= new RankInfo();
		return info;
	}
	
	public static void main(String args[]) {
		System.out.println("Java timer is about to start");
		String[] words = {"sportswear","clavier","karcher","chaussette pas cher"};
		GoogleSearchSaveTask beep = null;
		for (int i=0;i<words.length;i++){
			beep=new GoogleSearchSaveTask("test","test","test",words[i]);
		}

		System.out.println("All words launched !");
	}
}


