/**
 *
 */
package com.codeondemand.javapeppers.aleppo.common;

import com.codeondemand.javapeppers.aleppo.AleppoMessages;
import org.apache.logging.log4j.LogManager;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

/**
 * A DataCapsule is a basic unit data storage encapsulation. It is intended to
 * store named Objects. You should be able to implement any type of data
 * structure with this class since the data can be any type of object, including
 * another DataCapsule.
 * <p>
 * The basic concept of a DataCapsule is a named storage token. Every
 * DataCapsule has a Name and and it also has a Value stored by an Object. In
 * addition to the name and value, the DataCapsule contains MetaData such as
 * type and length, although obviously not all Objects stored by the DataCapsule
 * have the concept of length. There are a number of metadata keys (the strings
 * for retrieving the metadata) defined in AleppoConstants, although these can
 * be arbitrarily extended or reused by applications.
 * <p>
 * The standard metadata keys are:
 * <p>
 * ALEPPO_DC_MDATA_TYPE_KEY ALEPPO_DC_MDATA_LENGTH_KEY
 * ALEPPO_DC_MDATA_PRECISION_KEY ALEPPO_DC_MDATA_PROPERTIES_KEY
 * <p>
 * Objects stored by the DataCapsule can be any type of Object, even other
 * DataCapsules (although in that case, a RecordCapsule should be chosen. The
 * metadata that is stored by the DataCapsule are also arbitrary objects. *
 *
 * @author gfa
 */
public class DataCapsule implements Serializable {

    public DataCapsule(String name, Object data) {
        setName(name);
        setData(data);
    }

    public DataCapsule(String name, Object data, String type, int len) {
        setName(name);
        setData(data);
        setMetaData(AleppoConstants.ALEPPO_DC_MDATA_TYPE_KEY, type);
        setMetaData(AleppoConstants.ALEPPO_DC_MDATA_LENGTH_KEY, new Integer(len));
    }

    /**
     * Returns the name of the DataCapsule. Note that the name of a DataCapsule
     * cannot be null.
     *
     * @return The name associated with the DataCapsule instance.
     */
    public synchronized String getName() {
        return name;
    }

    /**
     * Associates a name with a DataCapsule. You cannot use this method to set a
     * null name, use reset instead.
     *
     * @param name A String representation for the DataCapsule name.
     */
    public synchronized boolean setName(String name) {
        boolean retval = false;
        if (name != null) {
            this.name = name;
            retval = true;
        } else {
            System.err.println(AleppoMessages.getString("DataCapsule.0")); //$NON-NLS-1$
        }
        return retval;
    }

    /**
     * Sets some meta-data to the DataCapsule. This meta-data is analogous to a
     * parameter in an XML tag.
     *
     * @param name  The name of the meta-data.
     * @param mdata The Object containing the meta-data. If this is null, but the
     *              item specified by name exists, then the item will be removed.
     * @return true if the meta-data was successfully added.
     */
    public synchronized boolean setMetaData(String name, Object mdata) {
        boolean retval = false;
        if (name != null) {
            if (mdata != null) {
                retval = putMetaData(name, mdata);
            } else {
                if (metadata.containsKey(name)) {
                    metadata.remove(name);
                }
            }
            retval = true;
        }
        return retval;
    }

    /**
     * Creates a copy of the DataCapsule (or RecordCapsule) that contains an
     * exact copy of the DataCapsule, including the metadata information. The
     * MetaData will be
     */
    public synchronized DataCapsule cloneDC() {
        DataCapsule retval = null;
        if (this instanceof RecordCapsule) {
            retval = new RecordCapsule(this.getName(), this.getData());
        } else {
            retval = new DataCapsule(this.getName(), this.getData());
        }

        // Clone the meta data also
        Iterator<Entry<String, Object>> i = this.getAllMetaData();
        while (i != null && i.hasNext()) {
            Entry<String, Object> e = i.next();
            retval.setMetaData(e.getKey(), e.getValue());
        }
        return retval;
    }

    /**
     * Indicates if there is a 'null' object stored as the value of the
     * DataCapsule.
     *
     * @return true if the DataCapsule stores a 'null' object.
     */
    public boolean isNull() {
        return isNull;
    }

    /**
     * Returns the object that encapsulates the data for this DataCapsule
     *
     * @return The Object holding the data for the DataCapsule
     */
    public synchronized Object getData() {
        return data;
    }

    /**
     * Sets the data Object associated with this DataCapsule
     *
     * @param data The Object that encapsulates the data associated with this
     *             DataCapsule.
     */
    public synchronized void setData(Object data) {

        this.data = data;
        logger.debug("Setting data for " + name + " : " + data);
        if (data == null) {
            isNull = true;
        } else {
            setMetaData(AleppoConstants.ALEPPO_DC_MDATA_TYPE_KEY, data.getClass().getCanonicalName());
            isNull = false;
        }
    }

    /**
     * Returns the name of the java class that holds the data for this
     * DataCapsule.
     *
     * @return A string representation of the class of the object that holds the
     * data for this DataCapsule.
     */
    public String getType() {
        String retval = null;
        if (data != null) {
            retval = data.getClass().toString();
        }
        return retval;
    }

    /**
     * Returns the Object associated with the MetaData key specified.
     *
     * @param key The String key for the MetaData object.
     * @return If there is an object stored by the MetaData key, then that
     * Object is returned, otherwise a 'null' is returned.
     */
    public synchronized Object getMetaData(String key) {
        Object retval = null;
        if (metadata != null) {
            retval = metadata.get(key);
        }
        return retval;
    }

    /**
     * This method basically clears out the data and meta-data that is
     * associated with the DataCapsule. If there is an underlying DataCapsule
     * that is holding the data, then it is cleared as well.
     *
     * @return true if the DataCapsule is reset
     */
    public synchronized boolean reset() {
        boolean retval = true;

        if (data instanceof DataCapsule) {
            retval = ((DataCapsule) data).reset();
        } else {
            if (data instanceof DataCapsule[]) {
                int len = ((DataCapsule[]) data).length;
                for (int i = 0; i < len; i++) {
                    ((DataCapsule[]) data)[i].reset();
                }
            }
        }

        // Clear out the variables for this DataCapsule and set it to
        // being a null DataCapsule.
        data = null;
        name = null;
        metadata = null;
        isNull = true;

        return retval;
    }

    /**
     * Returns an iterator over the set of MetaData keys, which can be used to
     * enumerate the keys, and/or fetch additional information about the
     * underlying MetaData
     *
     * @return An Iterator<Entry<String,Object>> is returned.
     */
    public Iterator<Entry<String, Object>> getAllMetaData() {
        Iterator<Entry<String, Object>> retval = null;

        if (metadata != null) {
            retval = metadata.entrySet().iterator();
        }
        return retval;
    }

    /**
     * Maintains the HashMap for the meta-data objects. Only create if needed
     * the first time.
     *
     * @param name  The name of the new metadata
     * @param value The Object holding the metadata.
     * @return true if the meta data can be added.
     */
    private synchronized boolean putMetaData(String name, Object value) {
        if (name != null) {
            if (metadata == null) {
                metadata = new HashMap<String, Object>();
            }
            metadata.put(name, value);
        }
        return true;
    }

    private HashMap<String, Object> metadata = null;
    protected String name = null;
    protected Object data = null;
    protected boolean isNull = false;

    /**
     * Just use the default implementation
     */
    private static final long serialVersionUID = 1L;
    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("DataCapsule");
}
