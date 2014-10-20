package vsm;

import java.util.Arrays;
import java.util.List;

import rita.wordnet.RiWordnet;

public class WordNetUtils {
	
	public static RiWordnet wordnet = new RiWordnet();
	public static List<String> poss = Arrays.asList(RiWordnet.VERB, RiWordnet.NOUN, RiWordnet.ADJ, RiWordnet.ADV);
	
	public static String getStem(String leaf){
		String[] stems;
		for(String pos : poss){
			stems = wordnet.getStems(leaf, pos);
			if (stems != null && stems.length > 0){
				return stems[0];
			}
		}
		return leaf;
	}
	
}
