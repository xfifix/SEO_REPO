package crawl4j.mongodb.testing;

import java.net.UnknownHostException;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

public class MongoDBBulkInsertionTest {
	public static void main(String[] args) throws UnknownHostException{
		MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
		DB db = mongoClient.getDB( "CRAWL4J" );
		DBCollection coll = db.getCollection("CRAWL_RESULTS");

		BulkWriteOperation builder = coll.initializeUnorderedBulkOperation();
		String url = "/maison/linge-maison/chemin-de-table-en-bambou-30cm-x-140cm-noir/f-117620202-auc3662800034158.html";
		String whole_text ="En continuant à naviguer sur notre site, vous acceptez l utilisation de cookies pour vous proposer des services et offres adaptés à vos centres d intérêts";
		String title = "Chemin de table en bambou 30cm x 140cm - Noir - Achat / Vente CHEMIN ET SET DE TABLE    - Cdiscount";

		BasicDBObject replacingObject = new BasicDBObject("url",url)
		.append("whole_text",whole_text)
		.append("title","qsdfqsdfqsdfqsdfazerazer");


		String new_url = "/maison/linge-maison/chemin-de-table-30cm-x-140cm-noir/f-117620202-auc3662800034158.html";
		String new_whole_text ="En continuant à naviguer sur notre site, vous acceptez l utilisation de cookies pour vous proposer des services et offres adaptés à vos centres d intérêts";
		String new_title = "Chemin de table en bambou 30cm x 140cm - Noir - Achat / Vente CHEMIN ET SET DE TABLE    - Cdiscount";

		BasicDBObject newObject = new BasicDBObject("url",new_url)
		.append("whole_text",new_whole_text)
		.append("title",new_title);


		//		links : ["/maison/v-117-0.html, /m-12401-beauville.html",
		//		         "/maison/linge-maison/linge-de-table/protections-pour-table/l-117620209.html",
		//		         "/maison/linge-maison/centre-de-table-brode-main-renaissance-sur-lin-78c/f-117620202-auc2009976066940.html",
		//		         "/maison/r-rouleau+chemin (...)"],
		//		h1 : "Chemin de table en bambou 30cm x 140cm - Noir",
		//		footer_extract : " Cap sur les bonnes affaires  avec le rayon maison Cdiscount ! Il est grand temps de se faire plaisir, le tout au meilleur prix !",
		//		ztd_extract : "Chemin de table en bambou 30cm x 140cm - Noir - Pour une décoration de table zen et naturelle ! Dimensions du chemin de table : 140 cm x 30 cm",
		//		short_description :"Chemin de table en bambou 30cm x 140cm - Noir - Pour une décoration de table zen et naturelle !",
		//		attributes :  [{attribute_name  = "Nom du produit",
		//		                attribute_value = "Chemin de table en bambou 30cm x 140cm - Noir"},
		//		               {attribute_name  = "Catégorie",
		//		                attribute_value = "CHEMIN ET SET DE TABLE"},
		//		               {attribute_name  = "Marque",
		//		                attribute_value = "AUCUNE"}],
		//		nb_attributes : 4,
		//		status_code : 200,
		//		headers : "Cache-Control: private@Cteonnt-Length: 59640@Content-Type: text/html; charset=utf-8@Vary: User-Agent@Set-Cookie: CdiscountPers",
		//		depth : 5,
		//		page_type : "FicheProduit",
		//		magasin : "maison",
		//		rayon : "linge-maison",
		//		page_rank : 0,
		//		last_update : "now"
		//		cdiscount_vendor : false,
		//		youtube_referenced : false,
		//		xpath1 : "    Chemin de table en bambou 30cm x 140cm - Noir",
		//		xpath2 : " ",
		//		xpath3 : " ",
		//		xpath4 : " ",
		//		xpath5 : " ",
		//		xpath6 : " ",
		//		xpath7 : " ",
		//		xpath8 : " ",
		//		xpath9 : " ",
		//		xpath10 : " ",
		//		produit : "Chemin de table en bambou 30cm x 140cm - Noir",
		//		category : "CHEMIN ET SET DE TABLE",
		//		brand : "AUCUNE",
		//		facettes : [{facette_name : "COULEUR",
		//		             facette_value : "ROSE",
		//		             facette_count : 3},
		//		             {facette_name : "MARQUE",
		//		             facette_value : "SAMSUNG",
		//		             facette_count : 90}
		//		])
		//	}
		//coll.insert(doc);

		try{
			builder.find(new BasicDBObject("url", new_url)).upsert().replaceOne(newObject);
			builder.find(new BasicDBObject("url", url)).upsert().replaceOne(replacingObject);
		}catch (Exception e){
			System.out.println("Trouble inserting : "+url);
			e.printStackTrace();  
		}

		BulkWriteResult result = builder.execute();
		System.out.println("Previous title : "+title);
		System.out.println("Results : "+result);

	}

}