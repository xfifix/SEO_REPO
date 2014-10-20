package vsm;

import java.io.File;
import java.io.FileReader;
import java.util.Properties;

public class IntermediateLoader {

	private static Properties properties;
	private static String rootPath = "/home/hesham/data/haitham/";
	public static void main(String[] args) {
		properties = new Properties();
		try {
			properties.load(new FileReader(new File(rootPath + "config/inter_properties")));
		} catch (Exception e) {
			System.out.println("Failed to load properties file!!");
			e.printStackTrace();
		}
		if (args.length == 0){
			System.out.println("YOU MUST PROVIDE A FILE NAME");
			return;
		}
		Document doc = Document.loadDocument(rootPath + properties.getProperty("data.collection_path") + "/" + args[0],
				   rootPath + properties.getProperty("config.stop_words_path"),
				   properties.getProperty("config.concept_level"),
				   properties.getProperty("config.wsd_type"),
				   new Integer(properties.getProperty("config.wsd_context")),
				   "true".equals(properties.getProperty("config.senses_log")),
				   rootPath + properties.getProperty("config.senses_log_dir")
		);
		doc.saveRepresentation(rootPath + properties.getProperty("config.inter_save_path"));
	}

}
