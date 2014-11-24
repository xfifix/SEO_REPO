package crawl4j.vsm;

import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class ProcessBetweenRayonSimilarity {

	private static String config_path = "/home/sduprey/My_Code/My_Java_Workspace/SIMILARITY_METRICS/config/";
	public static Properties properties;
	public  static File stop_words;
	private static String field_to_fetch;

	private static int nb_content = 0;

	private static Connection con = null;

	private static Map<String,List<String>> rayons_field = new ConcurrentHashMap<String,List<String>>();
	private static List<String> rayons = Collections.synchronizedList(new ArrayList<String>());


	private static double total_sum=0;
	private static double total_exact_number=0;

	public static void main(String[] args) {
		loadProperties();
		// getting the french stop words 
		stop_words = new File(properties.getProperty("config.stop_words_path"));
		field_to_fetch=properties.getProperty("data.fieldtofetch");
		String url = properties.getProperty("db.url");
		String user = properties.getProperty("db.user");
		String mdp = properties.getProperty("db.passwd");    
		// database variables
		try {
			con = DriverManager.getConnection(url, user, mdp);
			fetch_distinct_rayon();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Trouble with the database");
			System.exit(0);
		}

		// we instantiate as many threads as urls
		ExecutorService executor = Executors.newFixedThreadPool(rayons.size()/2);

		for (int i=0;i<rayons.size();i++){
			BetweenRayonWorkerThread rayonWorker;
			try {
				rayonWorker = new BetweenRayonWorkerThread(con,i,rayons, rayons_field, field_to_fetch);
				executor.execute(rayonWorker);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println("Trouble with the database for :"+rayons.get(i));
			}
		}
	}

	private static void fetch_distinct_rayon() throws SQLException {
		PreparedStatement local_pst = con.prepareStatement("SELECT distinct RAYON FROM CRAWL_RESULTS");
		ResultSet local_rs = local_pst.executeQuery();
		while (local_rs.next()) {
			String rayon =local_rs.getString(1);
			if ((!rayon.contains(".html"))&&(!rayon.contains("r-")&& (!rayon.contains("v-"))&& (!rayon.contains("sr")))){

				System.out.println("Adding rayon :"+rayon);
				rayons.add(rayon);
				List<String> field_list = rayons_field.get(rayon);
				if (field_list==null){
					field_list=new ArrayList<String>();
					rayons_field.put(rayon,field_list);
				}
				System.out.println("Adding rayon : "+ rayon);		
				// getting the URLs per rayon
				PreparedStatement field_pst;
				try {
					field_pst = con.prepareStatement("SELECT "+field_to_fetch+" FROM CRAWL_RESULTS WHERE RAYON='" +rayon+ "'");
					ResultSet field_rs = field_pst.executeQuery();
					while (field_rs.next()) {
						String field =field_rs.getString(1);
						field_list.add(field);
					}
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					System.out.println("Database trouble with the rayon :"+rayon);
					e.printStackTrace();
				}
			}
		}
	}




	public void saveResults(){
		//		System.out.println("writing files for rayon results");
		//		String rayon_file_output=properties.getProperty("data.rayon_output_file");
		//		BufferedWriter writer;
		//		try {
		//			writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(rayon_file_output), "UTF-8"));
		//			// we write the header
		//			writer.write("RAYON1;RAYON2;SIMILARITY_AVG\n");
		//			//saving rayon results
		//			//			Iterator<String> iter = unique_rayons.iterator();
		//			//			while (iter.hasNext()) {
		//			//				String rayoni = iter.next();
		//			//				Iterator<String> riter = unique_rayons.iterator();
		//			//				while (riter.hasNext()) {
		//			//					String rayonj = riter.next();
		//			//					RayonComparison raycomp = my_rayon_results.get(rayoni+rayonj);
		//			//					if (raycomp == null){
		//			//						raycomp=my_rayon_results.get(rayonj+rayoni);
		//			//					}
		//			//					if (raycomp != null){
		//			//						double avg = raycomp.getTotal()/raycomp.getCounter();
		//			//						System.out.println(rayoni);					
		//			//						System.out.println(rayonj);					
		//			//						System.out.println(avg);
		//			//						writer.write(rayoni+";"+rayonj+";"+avg +"\n");
		//			//					}
		//			//				}
		//			//			}
		//			writer.close();	
		//		} catch (UnsupportedEncodingException | FileNotFoundException e) {
		//			// TODO Auto-generated catch block
		//			e.printStackTrace();
		//		} catch (IOException e) {
		//			// TODO Auto-generated catch block
		//			e.printStackTrace();
		//		}	
		//		System.out.println("writing files for magasin results");
		//		String magasin_file_output=properties.getProperty("data.magasin_output_file");
		//		BufferedWriter magwriter;
		//		try {
		//			magwriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(magasin_file_output), "UTF-8"));
		//			// we write the header
		//			magwriter.write("MAGASIN1;MAGASIN2;SIMILARITY_AVG\n");
		//			//saving rayon results
		//			Iterator<String> iter = unique_magasins.iterator();
		//			while (iter.hasNext()) {
		//				String magasini = iter.next();
		//				Iterator<String> riter = unique_magasins.iterator();
		//				while (riter.hasNext()) {
		//					String magasinj = riter.next();
		//					MagasinComparison comp = my_magasin_results.get(magasini+magasinj);
		//					if (comp == null){
		//						comp=my_magasin_results.get(magasinj+magasini);
		//					}
		//					if (comp != null){
		//						double avg = comp.getTotal()/comp.getCounter();
		//						System.out.println(magasini);					
		//						System.out.println(magasinj);					
		//						System.out.println(avg);
		//						magwriter.write(magasini+";"+magasinj+";"+avg +"\n");
		//					}
		//				}
		//
		//			}
		//			magwriter.close();	
		//		} catch (UnsupportedEncodingException | FileNotFoundException e) {
		//			// TODO Auto-generated catch block
		//			e.printStackTrace();
		//		} catch (IOException e) {
		//			// TODO Auto-generated catch block
		//			e.printStackTrace();
		//		}	
	}


	private static void loadProperties(){
		properties = new Properties();
		try {
			properties.load(new FileReader(new File(config_path+"properties")));
		} catch (Exception e) {
			System.out.println("Failed to load properties file!!");
			e.printStackTrace();
		}
	}

	public String getProperty(String key){
		return properties.getProperty(key);
	}
	public Properties getProperties(){
		return properties;
	}
}