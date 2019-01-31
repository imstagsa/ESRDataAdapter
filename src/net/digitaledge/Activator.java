package net.digitaledge;

import net.sf.jasperreports.eclipse.AbstractJRUIPlugin;

import org.apache.log4j.Logger;
import org.eclipse.jface.resource.ImageDescriptor;
import org.eclipse.swt.graphics.Image;
import org.osgi.framework.BundleContext;

/**
 * The activator class controls the plug-in life cycle. By extending 
 * AbstractJRUIPlugin if offers also all the method to load SWT resources
 * without producing garbage. It offers also the functionalities to log
 * events
 * 
 */
public class Activator extends AbstractJRUIPlugin {

	/**
	 * The plugin ID. Not that if you want to change this value
	 * you should change also the Bundle-SymbolicName entry inside
	 * the MANIFEST.MF file
	 */
	public static final String PLUGIN_ID = "ElasticSearch REST Plugin";
	public static ESRIconDescriptor iconDescriptor = null;
	/**
	 * The shared instance of this plugin, it can be used to load
	 * resource of this plugin from outside, like images or log events.
	 */
	private static Activator plugin;
	
	/**
	 * The constructor
	 */
	public Activator() {
	}

	/**
	 * Called when the plugin is started inside the application.
	 * Can be used to do initialization of fields
	 */
	public void start(BundleContext context) throws Exception {
		super.start(context);
		plugin = this;
	}

	/**
	 * Called when the is stopped (often when the application is closed).
	 * Can be used to do release resources
	 */
	public void stop(BundleContext context) throws Exception {
		plugin = null;
		super.stop(context);
	}

	/**
	 * Returns the shared instance
	 *
	 * @return the shared instance
	 */
	public static Activator getDefault() {
		return plugin;
	}
	
	/**
	 * Return the id of this plugin
	 */
	@Override
	public String getPluginID() {
		return PLUGIN_ID;
	}
	
	public ImageDescriptor getImageDescriptor(String path) {

	 
	Logger.getLogger(Activator.class).debug("Getting image descriptor from: " + path);

	if (iconDescriptor == null) {
		iconDescriptor = new ESRIconDescriptor("datasource-elasticsearch");
	}

	return iconDescriptor.getIcon16();
	}
	
	public Image getImage(String path) {
		ImageDescriptor imageDesc = this.getImageDescriptor(path);
		return imageDesc.createImage();
	}
}