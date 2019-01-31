/****
 * 
 * Copyright 2013-2016 Wedjaa <http://www.wedjaa.net/>
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package net.digitaledge.adapter;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import net.digitaledge.ESRDataAdapterFactory;
import net.digitaledge.data.ESRDataConnection;
import net.digitaledge.data.ESRDataSource;
import net.sf.jasperreports.data.AbstractDataAdapterService;
import net.sf.jasperreports.engine.JRException;
import net.sf.jasperreports.engine.JRParameter;
import net.sf.jasperreports.engine.JasperReportsContext;


public class ESRAdapterService extends AbstractDataAdapterService {
    
	public final static String ES_HOST_PARAM = "elasticSearchHost";
	public final static String ES_PORT_PARAM = "elasticSearchPort";
	public final static String ES_CLUSTER_PARAM = "elasticSearchCluster";
	public final static String ES_USER_PARAM = "elasticSearchUsername";
	public final static String ES_PASSWORD_PARAM = "elasticSearchPassword";
	public final static String ES_INDEX_PARAM = "elasticSearchIndexes";
	public final static String ES_TYPE_PARAM = "elasticSearchTypes";
	public final static String ES_MODE_PARAM = "elasticSearchMode";
	
	private final ESRAdapter dataAdapter;
	private ESRDataSource ESRDataSource;
	private ESRDataConnection ESRDataConnection;
	
	private static final Logger logger = Logger.getLogger(ESRAdapterService.class);
    
	@SuppressWarnings("deprecation")
	public ESRAdapterService(JasperReportsContext jrContext, ESRAdapter dataAdapter) {
    	super(jrContext, dataAdapter);
        this.dataAdapter = dataAdapter;
        this.ESRDataSource = null;
    }

    @Override
    public void contributeParameters(Map<String, Object> parameters) throws JRException {
    	
    	logger.info("ESJAdapterService.contributeParameters");
    	
        if (ESRDataSource != null) {
            dispose();
        }
        if (dataAdapter != null) {
            try {
            	createJSONDataConnection();
                parameters.put(JRParameter.REPORT_CONNECTION, ESRDataConnection);
                //parameters.put(JRParameter.REPORT_PARAMETERS_MAP, 
                parameters.put(ESRAdapterService.ES_HOST_PARAM, dataAdapter.getElasticSearchHost());
                parameters.put(ESRAdapterService.ES_PORT_PARAM, dataAdapter.getElasticSearchPort());
                parameters.put(ESRAdapterService.ES_INDEX_PARAM, dataAdapter.getElasticSearchIndexes());
                parameters.put(ESRAdapterService.ES_MODE_PARAM, dataAdapter.getElasticSearchMode());
                parameters.put(ESRAdapterService.ES_TYPE_PARAM, dataAdapter.getElasticSearchTypes());
                parameters.put(ESRAdapterService.ES_USER_PARAM, dataAdapter.getElasticSearchUsername());
                parameters.put(ESRAdapterService.ES_PASSWORD_PARAM, dataAdapter.getElasticSearchPassword());
                parameters.put(ESRAdapterService.ES_CLUSTER_PARAM, dataAdapter.getElasticSearchCluster());
                //parameters.put(JRParameter.REPORT_DATA_SOURCE, jsonDataSource);
                
            } catch (Exception e) {
                throw new JRException(e);
            }
        }
    }
    
    private void createJSONDataConnection() throws JRException {
    	logger.info("ESRAdapterService.createJSONDataConnection");
    	ESRDataConnection = new ESRDataConnection(
   				dataAdapter.getElasticSearchIndexes(),
   				dataAdapter.getElasticSearchTypes(),
   				Integer.parseInt(dataAdapter.getElasticSearchMode()),
   				dataAdapter.getElasticSearchHost(),
   				Integer.parseInt(dataAdapter.getElasticSearchPort()),
   				dataAdapter.getElasticSearchUsername(),
   				dataAdapter.getElasticSearchPassword(),
   				dataAdapter.getElasticSearchCluster()
   			);
    	ESRDataConnection.setSearch("{\"query\":{\"match_all\":{}}}");
   }

    @Override
    public void dispose() {
    	logger.info("ESRAdapterService.dispose");
    	
    	if (ESRDataSource != null)
    		ESRDataSource = null;
    	
    	if (ESRDataConnection != null) {
    		try {
    			ESRDataConnection.close();
    		} catch (SQLException e) {
    			e.printStackTrace();
    		}
    	}
    }

    @Override
    public void test() throws JRException {
    	logger.info("ESRAdapterService.test");
        try {
            if (ESRDataSource != null) {
            } else {
            	createJSONDataConnection();
            	ESRDataSource = ESRDataConnection.createJSONDataSource();
            }
            ESRDataSource.test();
        } finally {
            dispose();
        }
    }
}
