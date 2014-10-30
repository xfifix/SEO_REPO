package com.multirequesting;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ExaleadThreadPool {
	private static int fixed_pool_size = 1;
	private static int size_bucket = 10;
	private static String input_path="D:\\My_Data\\My_Acquisition_Data\\top100k_new.csv";
	private static List<EntryInfo> tofetch_list = new ArrayList<EntryInfo>();

	public static void main(String[] args) throws SQLException {

		if (args.length==1){
			fixed_pool_size= Integer.valueOf(args[0]);
			System.out.println("You specified "+fixed_pool_size + " threads");
		}

		if (args.length==2){
			size_bucket= Integer.valueOf(args[1]);
			System.out.println("You specified a "+size_bucket + " bucket size");
		}

		if (args.length==3){
			input_path= args[2];
			System.out.println("You specified a "+input_path + " as input path");
		}		

		try {
			parse_input_file(input_path);
		} catch (NumberFormatException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Trouble reading the file or parsing it");
		}

		// it would be best to use a property file to store MD5 password
		//		// Getting the database property
		//		Properties props = new Properties();
		//		FileInputStream in = null;      
		//		try {
		//			in = new FileInputStream("database.properties");
		//			props.load(in);
		//		} catch (IOException ex) {
		//			Logger lgr = Logger.getLogger(HTTPStatusThreadPool.class.getName());
		//			lgr.log(Level.SEVERE, ex.getMessage(), ex);
		//
		//		} finally {
		//			try {
		//				if (in != null) {
		//					in.close();
		//				}
		//			} catch (IOException ex) {
		//				Logger lgr = Logger.getLogger(HTTPStatusThreadPool.class.getName());
		//				lgr.log(Level.SEVERE, ex.getMessage(), ex);
		//			}
		//		}
		// the following properties have been identified
		String url="jdbc:postgresql://localhost/EXALEADDB";
		String user="postgres";
		String passwd="root";


		System.out.println("You'll be using "+fixed_pool_size+" threads ");
		ExecutorService executor = Executors.newFixedThreadPool(fixed_pool_size);

		// The database connection
		Connection con = null;
		PreparedStatement pst = null;
		ResultSet rs = null;

		int size=tofetch_list.size();
		size=40;
		System.out.println("Dispatching "+size+" to "+fixed_pool_size+" threads");
		// we add one for the euclidean remainder
		int local_count=0;
		List<EntryInfo> thread_list = new ArrayList<EntryInfo>();
		for (int size_counter=0; size_counter<size;size_counter ++){
			if(local_count<size_bucket ){
				thread_list.add(tofetch_list.get(size_counter));
				local_count++;
			}
			if (local_count==size_bucket){
				Connection local_con = DriverManager.getConnection(url, user, passwd);
				System.out.println("Launching another thread with "+local_count+ " URLs to fetch");
				Runnable worker = new ExaleadRequestingWorkerThread(local_con,thread_list);
				executor.execute(worker);		
				local_count=0;
				thread_list = new ArrayList<EntryInfo>();
			}
		}
		if (thread_list.size()>0){
			Connection local_con = DriverManager.getConnection(url, user, passwd);
			System.out.println("Launching another thread with "+local_count+ " URLs to fetch");
			Runnable worker = new ExaleadRequestingWorkerThread(local_con,thread_list);
			executor.execute(worker);
		}
	}

	private static void parse_input_file(String inputPath) throws NumberFormatException, IOException{
		BufferedReader br = null;
		String line = "";
		String header = null;
		String[] column_names = null;
		String cvsSplitBy = ";";
		int nb_line=1;
		// last error
		br = new BufferedReader(new InputStreamReader(new FileInputStream(inputPath), "UTF-8"));	
		// we skip the first line : the headers
		header = br.readLine();
		column_names= header.split(cvsSplitBy);
		while ((line = br.readLine()) != null) {
			String[] splitted_line = line.split(cvsSplitBy);
			String request = splitted_line[0];
			int volume = 0;
			try {
				volume = Integer.valueOf(splitted_line[1]);
			} catch (NumberFormatException e){
				e.printStackTrace();
			}
			EntryInfo info =new EntryInfo();
			info.setRequest(request);
			info.setVolume(volume);
			System.out.println("Adding line : " +splitted_line[0]+" : "+ splitted_line[1]);
			tofetch_list.add(info);

			//			System.out.println("Title : "+title);
			//			System.out.println("counter : "+counter);
			//			System.out.println("URL : "+splitted_line[1]);
			//			System.out.println("Magasin : "+magasin);
			//			System.out.println("Rayon : "+rayon);
			//			System.out.println("Produit : "+produit);



		}
	}



}



