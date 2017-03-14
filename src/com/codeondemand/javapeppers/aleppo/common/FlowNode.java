/**
 * 
 */
package com.codeondemand.javapeppers.aleppo.common;

/**
* Revision History
* ----------------------------------------------------------------------------
* date      author      Description 
* --------- ----------- -------------------------------------------------------
* 03Dec2015 gfa			Added a getParameter convenience method.
* 
*/

import java.util.Observable;
import java.util.Properties;
import java.util.TreeMap;


import org.apache.logging.log4j.LogManager;

/**
 * The FlowNode is a abstract base class for any record processing node that is
 * executed by the com.javapeppers.aleppo framework.
 * 
 * It's only real function is to be initialized with the parameter map which
 * contains node specific data that allows granular customization of the nodes
 * behavior.
 * 
 * 
 * It also stores a RecordCapsule object containing information such as
 * properties and dynamic data that is passing through the dataflow channel.
 * 
 * @author gfa
 * 
 *
 */
public abstract class FlowNode extends Observable {

	/**
	 * Provides a mechanism to easily bring in key/value table (in a TreeMap
	 * format) which is set to the pmap variable.
	 */
	public boolean initialize(TreeMap<String, Object> map) {
		pmap = map;
		return doInitialization();
	}

	public void setProcessData(RecordCapsule data) {
		processData = data;
	}

	/**
	 * The implementing component should do any initialization that is required.
	 * All information that is needed should have been provided the 'protected'
	 * pmap variable. The pmap variable is accessible from subclasses, but not
	 * externally.
	 * 
	 * @return Return true if successfully initialized.
	 */
	public abstract boolean doInitialization();

	/**
	 * Sets the properties object to give the node a processing context.
	 * 
	 * @param props
	 *            A Properties object that contains any general data needed for
	 *            processing.
	 */
	public void setProperties(Properties props) {
		this.props = props;
		logger.debug("Properties set for " + this.toString());
	}

	/**
	 * Returns the property object associated with this object.
	 * 
	 * @return The Properties object for this class.
	 */
	public Properties getProperties() {
		return props;
	}

	/**
	 * Convenience method for getting a string value from the local parameter
	 * map.
	 * 
	 * @param name
	 *            The name of the parameter to retrieve.
	 * @return The string value of the parameter if it exists, otherwise null;
	 */
	protected String getParameter(String name) {
		String retval = null;
		if (pmap.containsKey(name)) {
			retval = (String) pmap.get(name);
		}
		return retval;
	}

	/**
	 * Properties to be set by the RecordConnector for use by the Reader to
	 * avoid having to duplicate effort reading properties. This will be set at
	 * initialization if the connector has a properties file specified.
	 */
	protected Properties props = null;

	protected TreeMap<String, Object> pmap = null;
	protected RecordCapsule processData = null;

	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("FlowNode");
}
