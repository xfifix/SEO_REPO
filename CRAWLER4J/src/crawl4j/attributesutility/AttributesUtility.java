package crawl4j.attributesutility;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class AttributesUtility {
	@SuppressWarnings("unchecked")
	public static String getAttributesJSONStringToStore(List<AttributesInfo> attributes_info){
		JSONArray attributesArray = new JSONArray();
		for (AttributesInfo info : attributes_info){
			JSONObject attributeObject = new JSONObject();
			attributeObject.put("attribute_name", info.getData_name());
			attributeObject.put("attribute_value", info.getData());
			attributesArray.add(attributeObject);
		}
		return attributesArray.toJSONString();
	}

	public static List<AttributesInfo> unserializeJSONString(String storedJSONString){
		List<AttributesInfo> to_return = new ArrayList<AttributesInfo>();
		JSONParser jsonParser = new JSONParser();
		try {
			JSONArray attributesArray = (JSONArray) jsonParser.parse(storedJSONString);
			@SuppressWarnings("rawtypes")
			Iterator i = attributesArray.iterator();
			while (i.hasNext()) {
				JSONObject innerObj = (JSONObject) i.next();
				AttributesInfo info = new AttributesInfo();
				info.setData_name((String)innerObj.get("attribute_name"));
				info.setData((String)innerObj.get("attribute_value"));
				to_return.add(info);
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return to_return;
	}
	
	public static Map<String,String> unserializeJSONStringtoAttributesMap(String storedJSONString){
		Map<String,String> to_return = new HashMap<String,String>();
		JSONParser jsonParser = new JSONParser();
		try {
			JSONArray attributesArray = (JSONArray) jsonParser.parse(storedJSONString);
			@SuppressWarnings("rawtypes")
			Iterator i = attributesArray.iterator();
			while (i.hasNext()) {
				JSONObject innerObj = (JSONObject) i.next();
				to_return.put((String)innerObj.get("attribute_name"),(String)innerObj.get("attribute_value"));
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return to_return;
	}
}
