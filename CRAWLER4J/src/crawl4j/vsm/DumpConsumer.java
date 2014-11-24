package crawl4j.vsm;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;



public class DumpConsumer {

	public static void main(String[] args) {
		Date start = new Date();
		//VectorSpace vs = VectorSpace.getDumpInstance();
		VectorSpace vs = VectorSpace.getInstance();
		
		List<Double> alphas = Arrays.asList(0.0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1.0);
		for (Double alpha : alphas){
			Document.resetAllSimilarities(alpha);
			vs.runClustering(new Kmeans(new Integer(vs.getProperty("data.kmeans_size"))));
		}

		alphas = Arrays.asList(0.0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1.0);
		for (Double alpha : alphas){
			Document.resetConceptSetting(alpha);
			vs.runClustering(new SingleLinkage(new Integer(vs.getProperty("data.hac_size"))));
		}
		
		Date end = new Date();
		System.out.print("completed execution in ");
		System.out.print(end.getHours() - start.getHours());
		System.out.print("hours, ");
		System.out.print(end.getMinutes() - start.getMinutes());
		System.out.print("minutes, ");
	}
	
}
