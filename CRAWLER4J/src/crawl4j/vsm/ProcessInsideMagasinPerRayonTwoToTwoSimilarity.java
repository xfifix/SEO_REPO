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


public class ProcessInsideMagasinPerRayonTwoToTwoSimilarity {
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
	private static String field_to_fetch;
	// threshold above which we deem the content too much similar !!
	//	private static double threshold = 0;
	private static Connection con = null;

	private static Map<String,List<String>> unique_rayons_per_magasin = new HashMap<String,List<String>>();
	private static Set<String> unique_magasins = new HashSet<String>();


	public static void main(String[] args) {
		loadProperties();
		// getting the french stop words 
		stop_words = new File(properties.getProperty("config.stop_words_path"));
		field_to_fetch=properties.getProperty("data.fieldtofetch");
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
			InsideMagasinBetweenRayonWorkerThread magasinWorker;
			try {
				magasinWorker = new InsideMagasinBetweenRayonWorkerThread(con,magasin,rayons,field_to_fetch);
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

//	public void saveResults(){
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
//			//			Iterator<String> iter = unique_magasins.iterator();
//			//			while (iter.hasNext()) {
//			//				String magasini = iter.next();
//			//				Iterator<String> riter = unique_magasins.iterator();
//			//				while (riter.hasNext()) {
//			//					String magasinj = riter.next();
//			//					MagasinComparison comp = my_magasin_results.get(magasini+magasinj);
//			//					if (comp == null){
//			//						comp=my_magasin_results.get(magasinj+magasini);
//			//					}
//			//					if (comp != null){
//			//						double avg = comp.getTotal()/comp.getCounter();
//			//						System.out.println(magasini);					
//			//						System.out.println(magasinj);					
//			//						System.out.println(avg);
//			//						magwriter.write(magasini+";"+magasinj+";"+avg +"\n");
//			//					}
//			//				}
//			//
//			//			}
//			magwriter.close();	
//		} catch (UnsupportedEncodingException | FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}	
//	}

	//	public void fetchData(String magasin){
	//		PreparedStatement pst = null;
	//		ResultSet rs = null;
	//		try {
	//			// we open the database
	//			pst = con.prepareStatement("SELECT "+field_to_fetch+" , URL, VENDOR, MAGASIN, RAYON, PRODUIT FROM CRAWL_RESULTS where magasin='"+magasin+"'");
	//			rs = pst.executeQuery();
	//			while (rs.next()) {
	//				nb_content++;
	//				URLContentInfo url_info = new URLContentInfo();
	//				String content = rs.getString(1);
	//				String my_url = rs.getString(2);
	//				String my_vendor = rs.getString(3);
	//				String my_magasin = rs.getString(4);
	//				String my_rayon = rs.getString(5);
	//				String my_produit = rs.getString(6);
	//				url_info.setContent(content);
	//				url_info.setUrl(my_url);
	//				url_info.setMagasin(my_magasin);
	//				url_info.setRayon(my_rayon);
	//				url_info.setProduit(my_produit);
	//				url_info.setVendor(my_vendor);
	//				my_infos.add(url_info);
	//			}    
	//		} catch (Exception ex) {
	//			Logger lgr = Logger.getLogger(ProcessInsideMagasinSimilarity.class.getName());
	//			lgr.log(Level.SEVERE, ex.getMessage(), ex);
	//		} finally {
	//			try {
	//				if (rs != null) {
	//					rs.close();
	//				}
	//				if (pst != null) {
	//					pst.close();
	//				}
	//				if (con != null) {
	//					con.close();
	//				}
	//			} catch (SQLException ex) {
	//				Logger lgr = Logger.getLogger(ProcessInsideMagasinSimilarity.class.getName());
	//				lgr.log(Level.WARNING, ex.getMessage(), ex);
	//			}
	//		}	
	//	}

	private static void loadProperties(){
		properties = new Properties();
		try {
			properties.load(new FileReader(new File(config_path+"properties")));
		} catch (Exception e) {
			System.out.println("Failed to load properties file!!");
			e.printStackTrace();
		}
	}

	//	public void buildComparisonMatrix(){
	//		int nb_contents = my_infos.size();
	//		System.out.println("Computing similarity distance : "+nb_contents);
	//		for (int i=0; i< nb_contents; i++){
	//			System.out.println(" percentage done : "+((double)i/nb_contents)*100+"%");
	//			System.out.println("Total sum : "+total_sum);
	//			System.out.println("Total number of comparisons : "+(nb_content*(nb_content+1)/2-nb_content));
	//			System.out.println("OVerall average VSM BOW similarity : "+total_sum/(nb_content*(nb_content+1)/2-nb_content));
	//			System.out.println("Total exact sum : "+total_exact_number);
	//			System.out.println("Total number of comparisons : "+(nb_content*(nb_content+1)/2-nb_content));
	//			System.out.println("OVerall average VSM BOW similarity : "+total_exact_number/(nb_content*(nb_content+1)/2-nb_content));
	//
	//			URLContentInfo infoi = my_infos.get(i);
	//			//			String rayoni = infoi.getRayon();
	//			//			String magasini = infoi.getMagasin();
	//			String contenti = infoi.getContent() == null ? "" : infoi.getContent();
	//			//			unique_magasins.add(magasini);
	//			//			unique_rayons.add(rayoni);
	//			for (int j=i+1; j<nb_contents; j++){
	//				URLContentInfo infoj = my_infos.get(j);
	//				//				String rayonj = infoj.getRayon();
	//				//				String magasinj = infoj.getMagasin();
	//				String contentj = infoj.getContent() == null ? "" : infoj.getContent();
	//				//System.out.println(contenti);
	//				//System.out.println(contentj);
	//				boolean isexactlysimilar = contenti.equals(contentj);
	//				double similarity = computeSimilarity(contenti,contentj);
	//				//System.out.println("i +"+i+" j + "+j+" = "+similarity);
	//				if (isexactlysimilar){
	//					total_exact_number++;
	//				}
	//				total_sum=total_sum+similarity;
	//
	//				//				if (similarity>=threshold){
	//				//					// storing each magasin result
	//				//					RayonComparison comp = my_rayon_results.get(rayoni+rayonj);
	//				//					if (comp == null){
	//				//						comp=new RayonComparison();
	//				//						comp.setRayoni(rayoni);
	//				//						comp.setRayonj(rayonj);
	//				//					}
	//				//					Double raytot = comp.getTotal();
	//				//					raytot=raytot+similarity;
	//				//					comp.setTotal(raytot);
	//				//					comp.increment();
	//				//					my_rayon_results.put(rayoni+rayonj, comp);
	//				//
	//				//					System.out.println(rayoni);
	//				//					System.out.println(rayonj);
	//				//					System.out.println(comp);
	//				//
	//				//					// storing each rayon result
	//				//					MagasinComparison mag_comp = my_magasin_results.get(magasini+magasinj);
	//				//					if (mag_comp == null){
	//				//						mag_comp=new MagasinComparison();
	//				//						mag_comp.setMagasini(magasini);
	//				//						mag_comp.setMagasinj(magasinj);
	//				//					}
	//				//					Double tot = mag_comp.getTotal();
	//				//					tot=tot+similarity;
	//				//					mag_comp.setTotal(tot);
	//				//					mag_comp.increment();
	//				//
	//				//					my_magasin_results.put(magasini+magasinj, mag_comp);
	//				//					System.out.println(magasini);
	//				//					System.out.println(magasinj);
	//				//					System.out.println(mag_comp);
	//				//
	//				//					// too costly for the time being
	//				//					//					// storing each element
	//				//					//					SparseElement my_elem = new SparseElement();
	//				//					//					my_elem.setPosition_i(i);
	//				//					//					my_elem.setPosition_j(j);
	//				//					//					my_elem.setValue(similarity);
	//				//					System.out.println(contenti);
	//				//					System.out.println(contentj);
	//				//					System.out.println("i +"+i+" j + "+j+" = "+similarity);
	//				//					//					my_url_results.add(my_elem);
	//				//				}
	//			}
	//		}
	//		System.out.println("Total sum : "+total_sum);
	//		System.out.println("Total number of comparisons : "+(nb_content*(nb_content+1)/2-nb_content));
	//		System.out.println("OVerall average VSM BOW similarity : "+total_sum/(nb_content*(nb_content+1)/2-nb_content));
	//
	//		System.out.println("Total exact sum : "+total_exact_number);
	//		System.out.println("Total number of comparisons : "+(nb_content*(nb_content+1)/2-nb_content));
	//		System.out.println("OVerall average VSM BOW similarity : "+total_exact_number/(nb_content*(nb_content+1)/2-nb_content));
	//
	//	}

//	private Double computeSimilarity(String text1, String text2) {
//		VectorStateSpringRepresentation vs1 =new VectorStateSpringRepresentation(text1);
//		VectorStateSpringRepresentation vs2 =new VectorStateSpringRepresentation(text2);
//		return cosine_similarity(vs1.getWordFrequencies() , vs2.getWordFrequencies());
//	}
//
//	private double cosine_similarity(Map<String, Integer> v1, Map<String, Integer> v2) {
//		Set<String> both = new HashSet<String>(v1.keySet());
//		both.retainAll(v2.keySet());
//		double sclar = 0, norm1 = 0, norm2 = 0;
//		for (String k : both) sclar += v1.get(k) * v2.get(k);
//		for (String k : v1.keySet()) norm1 += v1.get(k) * v1.get(k);
//		for (String k : v2.keySet()) norm2 += v2.get(k) * v2.get(k);
//		return sclar / Math.sqrt(norm1 * norm2);
//	}

	public String getProperty(String key){
		return properties.getProperty(key);
	}
	public Properties getProperties(){
		return properties;
	}
}