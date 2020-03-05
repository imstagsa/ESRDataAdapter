package net.digitaledge.data;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import net.sf.jasperreports.engine.JRDataSource;
import net.sf.jasperreports.engine.JRException;
import net.sf.jasperreports.engine.JRField;
import net.sf.jasperreports.engine.design.JRDesignField;


public class ESRDataSource implements JRDataSource {
	
    public static String QUERY_LANGUAGE = "esr-elastic";
    public static String ELASTIC_SEARCH = "es_search";
    
    public final static int ES_MODE_HITS = 0;
    public final static int ES_MODE_AGGS = 1;

    public final static String ES_DEFAULT_HOST = "";
    public final static int ES_DEFAULT_PORT = 0;
    public final static String ES_DEFAULT_CLUSTER = "";
    public final static int ES_DEFAULT_SEARCH_MODE = ES_MODE_HITS;
    private List<JRDesignField> jrDesignFieldList = new ArrayList<JRDesignField>();
    private List<Object[]> dataFieldList = new ArrayList<Object[]>();
    private static final Logger logger = Logger.getLogger(ESRDataSource.class);
 
	/**
	 * Variable to store current row
	 */
	private int counter = -1;

	
	public ESRDataSource(List<JRDesignField> jrDesignFieldList, List<Object[]> dataFieldList)
	{
		this.jrDesignFieldList = jrDesignFieldList;
		this.dataFieldList = dataFieldList;
	}
	
	@Override
	public Object getFieldValue(JRField jrField) throws JRException {
		try{
			for(int i = 0; i < jrDesignFieldList.size(); i++)
				if(jrField.getName().equals(jrDesignFieldList.get(i).getName()))
				{
					//System.out.println("FIELD: " + jrField.getName() + "  VALUE:" + dataFieldList.get(counter)[i] +"  CLASS: "+jrField.getValueClass().toString());
					return dataFieldList.get(counter)[i];
				}
		}catch (Exception e)
		{
			logger.debug("ESRDataSource.getFieldValue" + e.toString());
		}
		return "";
	}

	@Override
	public boolean next() throws JRException {
		if(dataFieldList.size() > 0)
			if (counter < dataFieldList.size()-1) {
				counter++;
				return true;
			}
		return false;
	}
	
	public void test() {
		// TODO Auto-generated method stub
	}
	
  	public List<JRDesignField> getJrDesignFieldList() {
		return jrDesignFieldList;
	}

	public void setJrDesignFieldList(List<JRDesignField> jrDesignFieldList) {
		this.jrDesignFieldList = jrDesignFieldList;
	}

}