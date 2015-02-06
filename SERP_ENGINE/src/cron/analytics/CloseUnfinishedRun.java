package cron.analytics;

import java.sql.SQLException;

public class CloseUnfinishedRun {
	public static void main(String[] args){
		try{	
			DataBaseManagement.instantiante_connection();
			int counter = DataBaseManagement.check_alive_run();
			if (counter >= 1){
				System.out.println("Another job is either running or badly finished, we'll cancel it");
				// closing the run by inserting a stopping date !
				DataBaseManagement.close_current_run();
			}
		} catch (SQLException e){
			e.printStackTrace();
		} finally{
			DataBaseManagement.close();
		}
	}
}

