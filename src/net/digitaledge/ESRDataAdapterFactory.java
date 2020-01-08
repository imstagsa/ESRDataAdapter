 package net.digitaledge;

import org.eclipse.swt.graphics.Image;
import org.apache.log4j.Logger;
import com.jaspersoft.studio.data.DataAdapterDescriptor;
import com.jaspersoft.studio.data.DataAdapterFactory;
import com.jaspersoft.studio.data.adapter.IDataAdapterCreator;

import net.digitaledge.adapter.ESRAdapter;
import net.digitaledge.adapter.ESRAdapterImplementation;
import net.digitaledge.adapter.ESRAdapterService;
import net.digitaledge.adapter.ESRDataAdapterCreator;
import net.sf.jasperreports.data.DataAdapter;
import net.sf.jasperreports.data.DataAdapterService;
import net.sf.jasperreports.engine.JasperReportsContext;

/**
 * This class provide information on the data adapter, like it's display name and icon
 * and has the capability to create it and its classes
 * 
 */
public class ESRDataAdapterFactory implements DataAdapterFactory {
	
	private static final Logger logger = Logger.getLogger(ESRDataAdapterFactory.class);

	/**
	 * Creates a new instance of the data adapter
	 * 
	 * @return a not null instance of the data adapter
	 */
	@Override
	public DataAdapterDescriptor createDataAdapter() {
		logger.debug("Creating DataAdapter");
		ESRDataAdapterDescriptor descriptor = new ESRDataAdapterDescriptor();
        descriptor.getDataAdapter().setElasticSearchIndexes("*");
        descriptor.getDataAdapter().setElasticSearchTypes("doc");
        descriptor.getDataAdapter().setElasticSearchHost("localhost");
        descriptor.getDataAdapter().setElasticSearchPort("9200");
        //descriptor.getDataAdapter().setElasticSearchCluster("elasticsearch");
        descriptor.getDataAdapter().setElasticSearchUsername(null);
        descriptor.getDataAdapter().setElasticSearchPassword(null);
        descriptor.getDataAdapter().setElasticSearchMode("0");
		return descriptor;
	}
	
	/**
	 * This method returns the class name of the DataAdapter implementation. This is used from the code that must check if
	 * this connection factory is the good one to instance the connection serialized with a specific class name. Since due
	 * to the ClassLoading limitation JSS may not be able to instance the class by its self, it looks for the appropriate
	 * registered DataAdapterFactory
	 * 
	 * @return the class name of the DataAdapter implementation created by this factory class
	 */
	@Override
	public String getDataAdapterClassName() {
		return ESRAdapterImplementation.class.getName();
	}

	/**
	 * This method provides the label of the data adapter type. I.e.: JDBC connection.
	 * 
	 * @return a not null and not empty string
	 */
	@Override
	public String getLabel() {
		return "ElasticSearch REST DataAdapter";
	}

	/**
	 * This method provides a short description of the data adapter type. I.e.: connection to a database using JDBC
	 * 
	 * @return a not null string
	 */
	@Override
	public String getDescription() {
		 return "ElasticSearch REST DataAdapter";
	}

	/**
	 * This method provides an icon for this data adapter. 
	 * 
	 * @param size the size in pixel of the icon
	 * @return the icon image, can be null if the image is not available
	 */
	@Override
	public Image getIcon(int size) {
		if (size == 16) {
			return Activator.getDefault().getImage("images/elastic-16.png");
		}
		return null;
	}

	/**
	 * Return a converter that can be used to build a JSS data adapter from an iReport data adapter definition
	 * 
	 * @return the class to build a Jaspersoft Studio data adapter from it iReport configuration. It can
	 * be null if this function is not provided
	 */
	@Override
	public IDataAdapterCreator iReportConverter() {
		return new ESRDataAdapterCreator();
	}

	/**
	 * Verifies if the current data adapter factory is deprecated.
	 * 
	 * @return true if it was deprecated, false otherwise
	 */
	@Override
	public boolean isDeprecated() {
		return false;
	}

	@Override
	public DataAdapterService createDataAdapterService(JasperReportsContext jasperReportsContext, DataAdapter dataAdapter) {
		if (dataAdapter instanceof ESRAdapter)
            return new ESRAdapterService(jasperReportsContext, (ESRAdapter) dataAdapter);
		else return null;
	}
}