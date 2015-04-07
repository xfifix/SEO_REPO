package crawl4j.attributesutility;

import java.util.List;
import java.util.Map;

public class AttributesTest {


	public static void main(String[] args){


		String toUnSerialize = "[{\"attribute_value\":\"VANDOREN\",\"attribute_name\":\"Marque\"},{\"attribute_value\":\"Bec de saxophone Optimum\",\"attribute_name\":\"Nom du produit\"},{\"attribute_value\":\"BEC\",\"attribute_name\":\"Catégorie\"},{\"attribute_value\":\"Saxophone\",\"attribute_name\":\"Accessoire(s) pour\"},{\"attribute_value\":\"0 g\",\"attribute_name\":\"Poids net en kg\"}]";
		List<AttributesInfo> datas = AttributesUtility.unserializeJSONString(toUnSerialize);
		Map<String,String> zeMap = AttributesUtility.unserializeJSONStringtoAttributesMap(toUnSerialize);
		System.out.println(zeMap);
	}

}
