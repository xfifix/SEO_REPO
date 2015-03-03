package crawl4j.mongodb.testing;

import java.net.UnknownHostException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class MongoDBTesting {
	public static void main(String[] args) throws UnknownHostException{
		MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
		DB db = mongoClient.getDB( "mydb" );
		DBCollection coll = db.getCollection("my_collection");
		//		BasicDBObject doc = new BasicDBObject("name", "MongoDB")
		//        .append("type", "database")
		//        .append("count", 1)
		//        .append("info", new BasicDBObject("x", 203).append("y", 102));
		//        coll.insert(doc);
		DBObject myDoc = coll.findOne();
//		System.out.println(myDoc);
//
//		for (int i=0; i < 1000; i++) {
//			coll.insert(new BasicDBObject("i", i));
//		}
		
		System.out.println(coll.getCount());
		DBCursor cursor = coll.find();
		try {
		   while(cursor.hasNext()) {
		       System.out.println(cursor.next());
		   }
		} finally {
		   cursor.close();
		}
		BasicDBObject query = new BasicDBObject("i", 71);

		cursor = coll.find(query);

		try {
		   while(cursor.hasNext()) {
		       System.out.println(cursor.next());
		   }
		} finally {
		   cursor.close();
		}
		// find all where i > 50
		query = new BasicDBObject("i", new BasicDBObject("$gt", 50));

		cursor = coll.find(query);
		try {
		    while (cursor.hasNext()) {
		        System.out.println(cursor.next());
		    }
		} finally {
		    cursor.close();
		}
		query = new BasicDBObject("i", new BasicDBObject("$gt", 20).append("$lte", 30));
		cursor = coll.find(query);

		try {
		    while (cursor.hasNext()) {
		        System.out.println(cursor.next());
		    }
		} finally {
		    cursor.close();
		}
	}
}
