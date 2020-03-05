package net.digitaledge.data;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

/**
 * This class is parsing JSON string and providing basic functionality such as find an JSON Object or convert JSON to List<MapVariableValue>
 * @author esimacenco
 *
 */

public class ESRUtils {
	
	private final static Logger logger = Logger.getLogger(ESRUtils.class);
	private JSONParser parser = new JSONParser();
	
	public ESRUtils() {}
	
	public Object parse(String json)
	{
        Object obj;
		try {
			obj = parser.parse(json);
			return obj;
		
		} catch (Exception e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		return null;
	}
	
    public static Object findFirstObject(JSONObject jsonObject, String objectName)
    {
    	List<Object> parentObjects = new ArrayList<Object>();
    	for (Object childJSONObject : jsonObject.keySet())
    	{
    		String childObjectName = (String)childJSONObject;
    		Object childObjectValue = jsonObject.get(childObjectName);
    		
    		if(childObjectName.equals(objectName))
    		    return childObjectValue;
    		else 
    			parentObjects.add(childObjectValue);
    	}
    		
    	for(Object parentObject : parentObjects)
    	{
    		if(parentObject instanceof JSONObject)
    		{
    			Object obj;
    			if((obj = findFirstObject((JSONObject)parentObject, objectName)) != null) return obj;
    		}
    	}
    	return null;
    }
	
    public static Object findObject(JSONObject jsonObject, String objectName)
    {
    	try {
    		return jsonObject.get(objectName);
    	} catch (Exception e) {
    		logger.error(e.getMessage());
    		e.printStackTrace(); 
    	}
    	return null;
    }
    
    public static List<MapVariableValue> convertToMapVariableValue(JSONObject jsonObject)
    {
    	List<MapVariableValue> listOfObjects = new ArrayList<MapVariableValue>();
    	convertJSONObject(jsonObject, listOfObjects, "");
    	return listOfObjects;
    }
    
    private static String getFieldType(JSONObject jsonObject)
    {
    	for (Object childJSONObject : jsonObject.keySet()) {
    		String childObjectName = (String)childJSONObject;
    		Object childObjectValue = jsonObject.get(childObjectName);
    		if(childObjectName.equals("type"))
    		    return childObjectValue.toString();
    		if(childObjectName.equals("properties"))
    		    return childObjectName;
     	}
    	return null;
    }
    
    private static Boolean ifFieldExists(JSONObject jsonObject, String fieldName)
    {
    	for (Object childJSONObject : jsonObject.keySet()) {
    		String childObjectName = (String)childJSONObject;
    		if(childObjectName.equals(fieldName))
    		    return true;
     	}
    	return false;
    }
    
    public static List<MapVariableValue> getListOfTypes(JSONObject jsonObject, List<MapVariableValue> listJsonObjects, String parentFieldName)
    {
    	if(listJsonObjects == null)
    		listJsonObjects = new ArrayList<MapVariableValue>();
    	
    	try {
    		for (Object key : jsonObject.keySet()) {

    			String keyStr = (String)key;
    			Object keyValue = jsonObject.get(keyStr);
             
    			if(keyValue instanceof JSONObject)
    			{
    				if(keyStr.equals("properties") && ((JSONObject)keyValue).keySet().size() > 1 )
    				{
    					getListOfTypes((JSONObject)keyValue, listJsonObjects, parentFieldName );
    				}
    				else if(findObject((JSONObject)keyValue, "properties") != null && ((JSONObject)keyValue).keySet().size() == 1 )
    				{
    					getListOfTypes((JSONObject)keyValue, listJsonObjects, parentFieldName+ keyStr+".");
    				}
    				else if(findObject((JSONObject)keyValue, "type") != null)
    				{
       					String type  = getFieldType((JSONObject)keyValue);
    					String format = new String();
						if(type.equals("date") && ifFieldExists((JSONObject)keyValue, "format"))
							format =  (String) findObject((JSONObject)keyValue, "format");
    					listJsonObjects.add(new MapVariableValue(parentFieldName + keyStr, type, format));
    				}
    			}
    		}
     } catch (Exception e) {
    	 logger.error(e.getMessage());
         e.printStackTrace();
     }
    	return listJsonObjects;
    } 
    
    private static void convertJSONObject(JSONObject jsonObject, List<MapVariableValue> listOfObjects, String parentFieldName)
    {
    	try {
    	 for (Object key : jsonObject.keySet()) {

             String keyStr = (String)key;
             Object keyvalue = jsonObject.get(keyStr);
             
             if(keyvalue != null)
             {
				switch (keyvalue.getClass().toString()) {
					case "class org.json.simple.JSONArray":
					{
						/**
						 *  WINLOGBEAT >= 7.0, Field  "keywords" : ["Audit Success"] - Array does have only value, not key:value.
						 *  This is temporary workaround only for key winlog.keywords.
						 *  TO DO: implement support Array type fields
						 */
						if(keyStr.equals("keywords") && keyvalue != null)
						{
							String keystr = keyvalue.toString();
							keystr = keystr.replaceAll("\\[", "");
							keystr = keystr.replaceAll("\\]", "");
							keystr = keystr.replaceAll("\"", "");
							listOfObjects.add(new MapVariableValue(parentFieldName + keyStr, (String) keystr));
						}
						else
						{
							listOfObjects.add(new MapVariableValue(keyStr, ""));
							convertJsonArray((JSONArray)keyvalue, listOfObjects, keyStr);
						}
					}
             		break;
					case "class org.json.simple.JSONObject":
					{
			            if(keyStr.equals("_source"))
			            {
			            	listOfObjects.add(new MapVariableValue(keyStr, ""));
			            	convertJSONObject((JSONObject)keyvalue, listOfObjects, "");
			            }
			            else
			            {
			            	listOfObjects.add(new MapVariableValue(keyStr, ""));
			            	convertJSONObject((JSONObject)keyvalue, listOfObjects, parentFieldName+keyStr+".");
			            }
					}
                 	break;
					case "class java.lang.String":
					{
						 if(keyStr != null && parentFieldName != null)
	            			 listOfObjects.add(new MapVariableValue(parentFieldName + keyStr, (String) keyvalue));
					}
					break;
					default: 
					{
						if(keyStr != null && parentFieldName != null)
	            			 listOfObjects.add(new MapVariableValue(parentFieldName + keyStr, keyvalue.toString()));
					}
    				break;
				}
             }
         }
     } catch (Exception e) {
    	 logger.error(e.toString());
         e.printStackTrace();
     }
    } 
    
    private static void convertJsonArray(JSONArray  jsonArray, List<MapVariableValue> listOfObjects, String parentFieldName)
    {
    	try {
    		int arraySize =  jsonArray.size();
            for(int i=0; i< arraySize; i++){
            	
            	Object keyvalue = jsonArray.get(i);
            	
                if(keyvalue instanceof JSONArray)
                	convertJsonArray((JSONArray)keyvalue, listOfObjects, parentFieldName);
                else if(keyvalue instanceof JSONObject)
                	convertJSONObject((JSONObject)keyvalue, listOfObjects, parentFieldName+".");
            }
    	} catch (Exception e) {
    		logger.error(e.getMessage());
    		e.printStackTrace(); 
    	}
    }
       
}
