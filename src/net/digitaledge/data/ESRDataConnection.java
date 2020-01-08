package net.digitaledge.data;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

import net.sf.jasperreports.engine.JRException;
import net.sf.jasperreports.engine.design.JRDesignField;

public class ESRDataConnection implements Connection {
	
    private String queryString;
    private String cluster;
    private String indice;
    private String types;
    private String strIndexes;
    private String strTypes;
    private String username;
    private String password;
    private String ESHOST;
    private Integer ESPORT;
    private int searchMode;
    private List<JRDesignField> jrDesignFieldList = new ArrayList<JRDesignField>();
	private List<Object[]> dataFieldList = new ArrayList<Object[]>();  	
	private static List<String> AggrSearchFilter = Arrays.asList("buckets");
	private static List<String> LogsSearchFilter = Arrays.asList("hits", "hits"); 
	
	private final static Logger logger = Logger.getLogger(ESRDataConnection.class);
    
    public ESRDataConnection(String hostname, int port)
    {
    	logger.debug("ESRDataConnection.ESRDataConnection");
        this.ESHOST = hostname;
        this.ESPORT = port;
    }
    
    public ESRDataConnection(String hostname, int port, String indexes)
    {
    	logger.debug("ESRDataConnection.ESRDataConnection");
        this.ESHOST = hostname;
        this.ESPORT = port;
        this.indice = indexes;
    }
    
    public ESRDataConnection(String indexes, String types, int searchMode, String hostname, int port, String username, String password, String cluster)
    {
    	logger.debug("ESRDataConnection.ESRDataConnection");
        this.username = username;
        this.password = password;
        this.ESHOST = hostname;
        this.cluster = cluster;
        this.ESPORT = port;
        this.searchMode = searchMode;
        this.indice = indexes;
        this.types = types;
        this.strIndexes = indexes;
        this.strTypes = types;
    }
	
	public ESRDataConnection clone()
	{
		return new ESRDataConnection(strIndexes, strTypes, searchMode, ESHOST, ESPORT, username, password, cluster);
	}
	
	private String getNewLogs(String index, String query) {
		
		logger.debug("ESRDataConnection.getNewLogs: " + query);
		String json = new String();
	    Timestamp timestamp1 = new Timestamp(System.currentTimeMillis());
        StringBuilder stringBuilder = new StringBuilder("http://"+ESHOST+":"+ESPORT+ index);
        StringBuffer response = new StringBuffer();
        
        try{
        	//stringBuilder.append(URLEncoder.encode(username, "UTF-8"));
        	URL obj = new URL(stringBuilder.toString());
        	HttpURLConnection con = (HttpURLConnection) obj.openConnection();
        	con.setRequestMethod("GET");
        	con.setRequestProperty("User-Agent", "java");
        	con.setRequestProperty("Accept-Charset", "UTF-8");
        	con.setRequestProperty("Accept", "*/*");
        	con.setRequestProperty("Content-Type","application/json");
        	
        	//Adding authentication header if both username and password are not empty
        	if((this.username != null) && (this.password != null) && (!this.username.isEmpty()) && (!this.password.isEmpty()))
        	{
        		String auth = this.username + ":" + this.password;
        		byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.UTF_8));
        		String authHeaderValue = "Basic " + new String(encodedAuth);
        		con.setRequestProperty("Authorization", authHeaderValue);
        	}
        	
        	con.setDoOutput(true);
        	System.out.println("stringBuilder.toString(): " + stringBuilder.toString());
        	System.out.println("Query: " + query);
        	
        	if(query.length() > 0){
        		query = query + "\r\n";
        		con.setRequestProperty("Content-Length", Integer.toString(query.getBytes().length));
        		DataOutputStream wr = new DataOutputStream(con.getOutputStream());
        		wr.writeBytes(query);
        		wr.flush();
        		wr.close();
        	}
        	int responseCode = con.getResponseCode();
        	System.out.println("Response Code: " + responseCode + ". " + con.getResponseMessage());
        	
        	if(responseCode != 200)
        	{
        		logger.error("Response Code: " + responseCode + ". " + con.getResponseMessage());
        		return new String("");
        	}
        	else
        	{
        		BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
        		while ((json = in.readLine()) != null) {
        			response.append(json);
        		}
        		in.close();
        	}
        } catch(Exception e){logger.error(e.toString());}
        
        Timestamp timestamp3 = new Timestamp(System.currentTimeMillis());
        logger.debug("Spend time: " + (timestamp3.getTime() - timestamp1.getTime()) + "ms");
        return response.toString();
	}
	
    public ESRDataSource createJSONDataSource() throws JRException {

    	getFieldMapping();
    	
    	String json = getNewLogs("/" + indice + "/_search?pretty", queryString);
    	ESRUtils jSONParser = new ESRUtils();
    	Object obj =  jSONParser.parse(json);
		
    	if(searchMode == 1)
    		buildDatasetFromJSON(obj, AggrSearchFilter);
    	else
    		buildDatasetFromJSON(obj, LogsSearchFilter);
    	
    	return new ESRDataSource(jrDesignFieldList, dataFieldList);
    }

    private boolean getFieldMapping()
	{
    	if(searchMode == 0)
    	{
    		ESRUtils jSONParser = new ESRUtils();
    		List<String> MAPPING_FILTER = Arrays.asList(indice, "mappings", types, "properties"); 
    		String json = getNewLogs("/" + indice + "/_mapping/" + types + "?pretty", "");
    		Object obj =  jSONParser.parse(json);
    		obj =  ESRUtils.findFirstObject((JSONObject)obj, "mappings");
    		obj =  ESRUtils.findFirstObject((JSONObject)obj, "properties");
    		List<MapVariableValue> listJsonObject = ESRUtils.getListOfTypes((JSONObject)obj, null, "");
    		for(MapVariableValue mapVariableValue : listJsonObject)
    		{
    			JRDesignField jrfield = new JRDesignField();
    			jrfield.setName(mapVariableValue.getVariable());
    			jrfield.setValueClass(getClassByName(mapVariableValue.getValue()));
    			jrfield.setValueClassName(jrfield.getValueClass().getName());
    			jrfield.setDescription(mapVariableValue.getDescription());
    			jrDesignFieldList.add(jrfield);
    		}
    	}
    	else
    	{
    		JRDesignField jrfieldKey = new JRDesignField();
			jrfieldKey.setName("key");
			jrfieldKey.setValueClass(String.class);
			jrfieldKey.setValueClassName(String.class.getName());
			jrfieldKey.setDescription("String");
			
			JRDesignField jrfieldInt = new JRDesignField();
			jrfieldInt.setName("doc_count");
			jrfieldInt.setValueClass(Integer.class);
			jrfieldInt.setValueClassName(Integer.class.getName());
			jrfieldInt.setDescription("doc_count");
			
			jrDesignFieldList.add(jrfieldKey);
			jrDesignFieldList.add(jrfieldInt);
    	}
    	
    	//jrDesignFieldList.forEach(arg0 -> System.out.println(arg0.getName()));

		logger.debug("ESRDataConnection.getFieldMapping RETURNING ");
		return true;
	}
    
    public List<JRDesignField> getFieldsMapping()
    {
    	getFieldMapping();
    	return jrDesignFieldList;
    }
    /**
     * For testing purpose
     * @return
     */
    public int testData()
    {
       	String jsonData = getNewLogs("/" + indice + "/_search", queryString);
       	if(jsonData.length() > 0)
       	{
       		ESRUtils jSONParser = new ESRUtils();
       		jSONParser = new ESRUtils();
       		Object obj =  jSONParser.parse(jsonData);
       		return ((JSONObject)obj).keySet().size();
       	}
       	return 0;
    }
    /**
     * For testing purpose
     * @return
     */
    public int testDataDataset()
    {
    	
    	getFieldMapping();
       	String jsonData = getNewLogs("/" + indice + "/_search", queryString);
       	if(jsonData.length() > 0)
       	{
       		ESRUtils jSONParser = new ESRUtils();
       		jSONParser = new ESRUtils();
       		Object obj =  jSONParser.parse(jsonData);
       		buildDatasetFromJSON(obj, LogsSearchFilter);
           	return dataFieldList.size();
       	}
       	return 0;
    }
    /**
     * For testing purpose
     * @return
     */
    public int testDataDatasetAggr()
    {
    	
    	getFieldMapping();
       	String jsonData = getNewLogs("/" + indice + "/_search?size=0", queryString);
       	if(jsonData.length() > 0)
       	{
       		ESRUtils jSONParser = new ESRUtils();
       		jSONParser = new ESRUtils();
       		Object obj =  jSONParser.parse(jsonData);
       		
        	buildDatasetFromJSON(obj, AggrSearchFilter);
           	return dataFieldList.size();
       	}
       	return 0;
    }
    
    private void addRowToDataset(List<MapVariableValue> list)
    {
    	if(jrDesignFieldList.size() > 0)
    	{
    		Object[] objectArray = new Object[jrDesignFieldList.size()];
    		
    		for(MapVariableValue mapVariableValue : list)
    		{
    			int index = 0;
    			Class cls = null;
    			String description = null;
    			
    			for(int i = 0; i < jrDesignFieldList.size(); i++)
    				if(mapVariableValue.getVariable().equals(jrDesignFieldList.get(i).getName()))
    				{
    					index = i;
    					cls = jrDesignFieldList.get(i).getValueClass();
    					description = jrDesignFieldList.get(i).getDescription();
    				}
    			
    				if(cls != null)
    				{
    					try{
    						switch (cls.toString()) {
						
								case "class java.lang.String": objectArray[index] = mapVariableValue.getValue();
	                    			break;
						
								case "class java.lang.Long":  objectArray[index] = Long.valueOf(mapVariableValue.getValue());
                    				break;
						
								case "class java.lang.Integer":  objectArray[index] = Integer.valueOf(mapVariableValue.getValue());
                					break;
                				
								case "class java.lang.Short":  objectArray[index] = Short.valueOf(mapVariableValue.getValue());
            						break;
            			
								case "class java.lang.Byte":  objectArray[index] = Byte.valueOf(mapVariableValue.getValue());
            						break;
            			
								case "class java.lang.Double":  objectArray[index] = Double.valueOf(mapVariableValue.getValue());
            						break;
            			
								case "class java.lang.Float":  objectArray[index] = Float.valueOf(mapVariableValue.getValue());
            						break;
            			
								case "class java.sql.Date":  objectArray[index] = convertStringToSQLDate(mapVariableValue, description.trim());
            						break;
            			
								case "class java.lang.Boolean":  objectArray[index] = Boolean.valueOf(mapVariableValue.getValue());
            						break;
            			
								default:  objectArray[index] = mapVariableValue.getValue();
                    				break;
    						}
    					} catch(Exception e){logger.debug(e.toString());}
    				}
    			}
    			
    		dataFieldList.add(objectArray);
    	}
    }
    
	private java.sql.Date convertStringToSQLDate(MapVariableValue field, String format)
	{
		/**
		 * By default field @timestamp doesn't have format field and usually has pattern "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
		 */
		if(field.getVariable().equals("@timestamp"))
		{
			String ISO_8601_24H_FULL_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
			SimpleDateFormat sdf1 = new SimpleDateFormat(ISO_8601_24H_FULL_FORMAT);
			
			java.util.Date date = new java.util.Date();
			try {
				date = sdf1.parse(field.getValue().trim());
				return new java.sql.Date(date.getTime());
			} catch (ParseException e) {
				logger.debug(e.toString());
			}
		}
		else
		{
			String[] dateFormats = format.split("\\|");
			
			if(format.length() > 0)
			{
			
				for(int i = 0; i < dateFormats.length; i++)
				{
					logger.debug("Trying to pasrse " + field.getValue().trim() + " with format: " + dateFormats[i]);
					SimpleDateFormat sdf1 = new SimpleDateFormat(dateFormats[i]);
					java.util.Date date = new java.util.Date();
					try {
						date = sdf1.parse(field.getValue().trim());
						return new java.sql.Date(date.getTime());
					} catch (ParseException e) {
						logger.debug(e.toString());
					}
				}
			}
			else
			{
				logger.debug("Trying to pasrse " + field.getValue().trim() + " with format: yyyy-MM-dd'T'HH:mm:ss.SSS");
				SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
				java.util.Date date = new java.util.Date();
				try {
					date = sdf1.parse(field.getValue().trim());
					return new java.sql.Date(date.getTime());
				} catch (ParseException e) {
					logger.debug(e.toString());
				}
			}
		}
		return null;
	}
	
    private Class getClassByName(String classStr)
    {
    	Class<?> act = null; 
    	try{
			switch (classStr) {
			
				case "text":  act = Class.forName("java.lang.String");
            		break;
			
				case "keyword":  act = Class.forName("java.lang.String");
        			break;
			
				case "long":  act = Class.forName("java.lang.Long");
        			break;
			
				case "integer":  act = Class.forName("java.lang.Integer");
    				break;
    				
				case "short":  act = Class.forName("java.lang.Short");
					break;
			
				case "byte":    act = Class.forName("java.lang.Byte");
					break;
			
				case "double": act = Class.forName("java.lang.Double");
					break;
			
				case "float":  act = Class.forName("java.lang.Float");
					break;
			
				case "date":   act = Class.forName("java.sql.Date");
					break;
			
				case "boolean":  act = Class.forName("java.lang.Boolean");
					break;
			
				case "binary": act = Class.forName("java.lang.String");
					break;
			
				case "half_float":  act = Class.forName("java.lang.Float");//Todo
					break;
			
				case "scaled_float":  act = Class.forName("java.lang.Float");//Todo
					break;
			
				default:  act = Class.forName("java.lang.Object");
        			break;
			}
		} catch(Exception e){ }
    	
    	return act;
    }
    
	private void buildDatasetFromJSON(Object obj, List<String> filter)
	{

		//for(int i = 0; i < filter.size(); i++)
		//	obj =  ESRUtils.findObject((JSONObject)obj, filter.get(i));

		for(int i = 0; i < filter.size(); i++)
			obj =  ESRUtils.findFirstObject((JSONObject)obj, filter.get(i));
		
		if(obj instanceof org.json.simple.JSONArray)
		{
			for(int i=0; i < ((org.json.simple.JSONArray)obj).size(); i++)
			{
				List<MapVariableValue> list = ESRUtils.convertToMapVariableValue((JSONObject)((org.json.simple.JSONArray)obj).get(i));
				addRowToDataset(list);
			}
		}
	}
    
    public void setSearch(String search)
    {
        queryString = search;
    }
    
    public List<JRDesignField> getJrDesignFieldList() {
		return jrDesignFieldList;
	}

	public void setJrDesignFieldList(List<JRDesignField> jrDesignFieldList) {
		this.jrDesignFieldList = jrDesignFieldList;
	}
	
    public List<Object[]> getDataFieldList() {
		return dataFieldList;
	}

	public void setDataFieldList(List<Object[]> dataFieldList) {
		this.dataFieldList = dataFieldList;
	}

	@Override
	public boolean isWrapperFor(Class<?> arg0) throws SQLException {
		logger.debug("ESRDataConnection.isWrapperFor");
		return false;
	}

	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException {
		logger.debug("ESRDataConnection.unwrap");
		return null;
	}

	@Override
	public void abort(Executor arg0) throws SQLException {
		logger.debug("ESRDataConnection.abort");
	}

	@Override
	public void clearWarnings() throws SQLException {
		logger.debug("ESRDataConnection.clearWarnings");
		
	}

	@Override
	public void close() throws SQLException {
		logger.debug("ESRDataConnection.close");
	}

	@Override
	public void commit() throws SQLException {
		logger.debug("ESRDataConnection.commit");
	}

	@Override
	public Array createArrayOf(String arg0, Object[] arg1) throws SQLException {
		logger.debug("ESRDataConnection.createArrayOf");
		return null;
	}

	@Override
	public Blob createBlob() throws SQLException {
		logger.debug("ESRDataConnection.createBlob");
		return null;
	}

	@Override
	public Clob createClob() throws SQLException {
		logger.debug("ESRDataConnection.createClob");
		return null;
	}

	@Override
	public NClob createNClob() throws SQLException {
		logger.debug("ESRDataConnection.createNClob");
		return null;
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		logger.debug("ESRDataConnection.createSQLXML");
		return null;
	}

	@Override
	public Statement createStatement() throws SQLException {
		logger.debug("ESRDataConnection.createStatement");
		return null;
	}

	@Override
	public Statement createStatement(int arg0, int arg1) throws SQLException {
		logger.debug("ESRDataConnection.createStatement");
		return null;
	}

	@Override
	public Statement createStatement(int arg0, int arg1, int arg2) throws SQLException {
		logger.debug("ESRDataConnection.createStatement");
		return null;
	}

	@Override
	public Struct createStruct(String arg0, Object[] arg1) throws SQLException {
		logger.debug("ESRDataConnection.createStruct");
		return null;
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		logger.debug("ESRDataConnection.getAutoCommit");
		return false;
	}

	@Override
	public String getCatalog() throws SQLException {
		logger.debug("ESRDataConnection.getCatalog");
		return null;
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		logger.debug("ESRDataConnection.getClientInfo");
		return null;
	}

	@Override
	public String getClientInfo(String arg0) throws SQLException {
		logger.debug("ESRDataConnection.getClientInfo");
		return null;
	}

	@Override
	public int getHoldability() throws SQLException {
		logger.debug("ESRDataConnection.getHoldability");
		return 0;
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		logger.debug("ESRDataConnection.getMetaData");
		return null;
	}

	@Override
	public int getNetworkTimeout() throws SQLException {
		logger.debug("ESRDataConnection.getNetworkTimeout");
		return 0;
	}

	@Override
	public String getSchema() throws SQLException {
		logger.debug("ESRDataConnection.getSchema");
		return null;
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		logger.debug("ESRDataConnection.getTransactionIsolation");
		return 0;
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		logger.debug("ESRDataConnection.getTypeMap");
		return null;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		logger.debug("ESRDataConnection.getWarnings");
		return null;
	}

	@Override
	public boolean isClosed() throws SQLException {
		logger.debug("ESRDataConnection.isClosed");
		return false;
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		logger.debug("ESRDataConnection.isReadOnly");
		return false;
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		logger.debug("ESRDataConnection.isValid");
		return false;
	}

	@Override
	public String nativeSQL(String sql) throws SQLException {
		logger.debug("ESRDataConnection.nativeSQL");
		return null;
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		logger.debug("ESRDataConnection.prepareCall");
		return null;
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		logger.debug("ESRDataConnection.prepareCall");
		return null;
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		logger.debug("ESRDataConnection.prepareCall");
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		logger.debug("ESRDataConnection.prepareStatement");
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		logger.debug("ESRDataConnection.prepareStatement");
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		logger.debug("ESRDataConnection.prepareStatement");
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		logger.debug("ESRDataConnection.prepareStatement");
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
			throws SQLException {
		logger.debug("ESRDataConnection.prepareStatement");
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		logger.debug("ESRDataConnection.prepareStatement");
		return null;
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		logger.debug("ESRDataConnection.releaseSavepoint");
		
	}

	@Override
	public void rollback() throws SQLException {
		logger.debug("ESRDataConnection.rollback");
		
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		logger.debug("ESRDataConnection.rollback");
		
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		logger.debug("ESRDataConnection.setAutoCommit");
		
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		logger.debug("ESRDataConnection.setCatalog");
		
	}

	@Override
	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		logger.debug("ESRDataConnection.setClientInfo");
		
	}

	@Override
	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		logger.debug("ESRDataConnection.setClientInfo");
		
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
		logger.debug("ESRDataConnection.setHoldability");
		
	}

	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
		logger.debug("ESRDataConnection.setNetworkTimeout");
		
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		logger.debug("ESRDataConnection.setReadOnly");
		
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		logger.debug("ESRDataConnection.setSavepoint");
		return null;
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		logger.debug("ESRDataConnection.setSavepoint");
		return null;
	}

	@Override
	public void setSchema(String schema) throws SQLException {
		logger.debug("ESRDataConnection.setSchema");
		
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
		logger.debug("ESRDataConnection.setTransactionIsolation");
		
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		logger.debug("ESRDataConnection.setTypeMap");
		
	}
	
}
