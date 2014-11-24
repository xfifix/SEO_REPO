package crawl4j.vsm;

import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class ProcessInsideMagasinPerRayonQualityScore {
	private static String[] magasins ={
		"animalerie",
		"bijouterie",
		"musique-instruments",
		"vetements-bebe",
		//	"bricolage-chauffage",
		//	"lingerie-feminine",
		"musique-cd-dvd",
		//	"jardin-animalerie",
		"le-sport",
		"maison",
		"telephonie",
		//	"vin-alimentaire",
		"boutique-erotique",
		//	"cadeaux-noel",
		"juniors",
		"tout-a-moins-de-10-euros",
		"arts-loisirs",
		//	"jeux-educatifs",
		"vetements-homme",
		"cosmetique",
		"sante-mieux-vivre",
		//shippinginformation
		//carte-cdiscount
		"edito",
		"clemarche",
		//"CmesApps",
		//"cmesapps",
		//abonnements
		"boutique-cadeaux",
		"soldes-promotions",
		"destockage",
		"vetements-femme",
		"pret-a-porter",
		"informatique",
		"jeux-pc-video-console",
		"livres-bd",
		"dvd",
		"electromenager",
		"bagages",
		//"vetements-enfant",
		"au-quotidien",
		"vin-champagne",
		"jardin",
		"bebe-puericulture",
		"photo-numerique",
		//"culture-multimedia",
		"personnalisation-3d",
		"chaussures",
		"auto",
	"high-tech"};
	private static String config_path = "/home/sduprey/My_Code/My_Java_Workspace/SIMILARITY_METRICS/config/";
	public static Properties properties;
	public  static File stop_words;

	// threshold above which we deem the content too much similar !!
	//	private static double threshold = 0;
	private static Connection con = null;

	private static Map<String,List<String>> unique_rayons_per_magasin = new HashMap<String,List<String>>();
	private static Set<String> unique_magasins = new HashSet<String>();


	public static void main(String[] args) {
		loadProperties();
		// getting the french stop words 
		stop_words = new File(properties.getProperty("config.stop_words_path"));
		//magasin_to_filter=properties.getProperty("data.magasintofilter");
		String url = properties.getProperty("db.url");
		String user = properties.getProperty("db.user");
		String mdp = properties.getProperty("db.passwd");    
		// database variables
		try {
			con = DriverManager.getConnection(url, user, mdp);
			fetch_distinct_magasin_and_rayon();
			//fetch_rayon();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Trouble with the database");
			System.exit(0);
		}

		// we instantiate as many threads as urls
		ExecutorService executor = Executors.newFixedThreadPool(magasins.length);


		for (int i=0;i<magasins.length;i++){
			String magasin = magasins[i];
			List<String> rayons = unique_rayons_per_magasin.get(magasin);
			InsideMagasinQualityWorkerThread magasinWorker;
			try {
				magasinWorker = new InsideMagasinQualityWorkerThread(con,magasin,rayons);
				executor.execute(magasinWorker);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println("Trouble with the database for :"+magasin);
			}


		}
	}


	private static void fetch_distinct_magasin_and_rayon() throws SQLException{
		PreparedStatement pst = con.prepareStatement("SELECT MAGASIN, count(*) FROM CRAWL_RESULTS GROUP BY MAGASIN");
		ResultSet rs = pst.executeQuery();
		while (rs.next()) {
			String magasin =rs.getString(1);
			int magasin_count=rs.getInt(2);
			if (magasin_count > 1){
				if ((!magasin.contains(".html"))&&(!magasin.contains("lf-")&& (!magasin.contains("mpv-"))&& (!magasin.contains("sr"))&& (!magasin.contains("ct-"))&& (!magasin.contains("sn-")))){
					System.out.println("Adding magasin : "+ magasin);
					unique_magasins.add(magasin);
					PreparedStatement local_pst = con.prepareStatement("SELECT RAYON, count(*) FROM CRAWL_RESULTS WHERE MAGASIN='"+magasin+"' GROUP BY RAYON");
					ResultSet local_rs = local_pst.executeQuery();
					List<String> rayon_list = unique_rayons_per_magasin.get(magasin);
					if (rayon_list==null){
						rayon_list=new ArrayList<String>();
						unique_rayons_per_magasin.put(magasin,rayon_list);
					}
					while (local_rs.next()) {
						String rayon =local_rs.getString(1);
						int rayon_count=local_rs.getInt(2);
						if (rayon_count>1){

							System.out.println("Adding rayon : "+ rayon +rayon_count);					
							rayon_list.add(rayon);
						}
					}
				}
			}
		}
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
