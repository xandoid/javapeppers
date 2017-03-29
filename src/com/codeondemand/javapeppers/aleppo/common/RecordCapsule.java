/**
 *
 */
package com.codeondemand.javapeppers.aleppo.common;

import org.apache.logging.log4j.LogManager;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * The RecordCapsule class is the container for the basic unit of work for the
 * Aleppo dataflow framework. Each data record (or process record if the Aleppo
 * framework is used for process control rather than data flow) is encapsulated
 * in a RecordCapsule. The RecordCapsule will contain some number of
 * DataCapsules (these map to a field level of a data flow, or some arbitrary
 * block of information in a process flow).
 *
 * @author gfa
 */
public class RecordCapsule extends DataCapsule {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    public RecordCapsule(String name, Object data) {
        super(name, data);
    }

    /**
     * THis is used to determine if two RecordCapsules contain information
     * related to the same 'thing' as determined by the contents of the 'key'
     * for each of the RecordCapsules. This is useful when there are
     * 'aggregation' or 'concentration' nodes in a data flow, so the two
     * RecordCapsules can be combined.
     *
     * @param rc2 The second RecordCapsule to compare with the current
     *            RecordCapsule
     * @return true if the keys compare with the specified RecordCapsule
     */
    public boolean compareKeys(RecordCapsule rc2) {
        boolean retval = true;
        if (this.keys.size() == rc2.keys.size()) {
            for (int i = 0; i < keys.size(); i++) {
                if (keys.get(i).getField_position() != rc2.keys.get(i).getField_position()) {
                    retval = false;
                    break;
                }
                if (retval) {
                    int pos = keys.get(i).getField_position();
                    DataCapsule dc1 = fields.get(pos);
                    DataCapsule dc2 = rc2.fields.get(pos);
                    retval = retval && dc1.getData().toString().equals(dc2.getData().toString());
                }
            }
        } else {
            retval = false;
        }
        ;
        return retval;
    }

    /**
     * Returns a String representation of the key for this RecordCapsule. This
     * is done by concatenating the String representations of all DataCapsules
     * that are keys for this RecordCapsule.
     *
     * @return A String representation of the key for this RecordCapsule.
     */
    public String getKeyString() {
        StringBuffer sb = new StringBuffer();
        for (KeySpecification key : keys) {
            int pos = key.getField_position();
            DataCapsule dc = fields.get(pos);
            if (!dc.isNull) {
                sb.append(dc.getData().toString());
            }
        }
        return sb.toString();
    }

    /**
     * Returns a DataCapsule given a field position specification.
     *
     * @param idx The positional index of the DataCapsule that is desired.
     * @return A DataCapsule at position specified by idx if it exists,
     * otherwise null.
     */
    public DataCapsule getField(int idx) {
        DataCapsule retval = null;
        if (idx < fields.size()) {
            retval = fields.get(idx);
        }
        return retval;
    }

    /**
     * Remove a DataCapsule with the specified name.
     *
     * @param name The Name of the DataCapsule to remove.
     * @return Returns true if successful.
     */
    public boolean removeField(String name) {
        boolean found = false;
        int idx = -1;
        String temp = name.trim();
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).getName().equals(temp)) {
                idx = i;
                found = true;
                break;
            }
        }
        if (found) {
            fields.remove(idx);
        }
        return found;
    }

    /**
     * Fetches a DataCapsule with a specified name.
     *
     * @param name The name of the DataCapsule to fetch.
     * @return A DataCapsule of the specified name, or null if it is not found.
     */
    public DataCapsule getField(String name) {
        DataCapsule retval = null;
        for (DataCapsule field : fields) {
            if (field.getName().trim().equals(name.trim())) {
                retval = field;
            }
        }

        return retval;
    }

    /**
     * Checks to see if a field exists and contains a non-null data object.
     * (note the data contained in the object might be empty)
     *
     * @return Returns true if the named field exists and has a non-null
     * DataCapsule attached.
     */
    public boolean checkField(String name) {
        boolean retval = true;
        if (getField(name) == null || getField(name).isNull()) {
            retval = false;
        }
        return retval;
    }

    /**
     * Determines if a DataCapsule specified by a name is a key for this
     * RecordCapsule.
     *
     * @param name The name of the DataCapsule.
     * @return true if the DataCapsule is a key, or false if it does not exist
     * or is not a key.
     */
    public boolean isKey(String name) {
        boolean retval = false;
        for (KeySpecification ks : keys) {
            if (ks.getName() != null && ks.getName().equals(name)) {
                retval = true;
                break;
            }
        }
        return retval;
    }

    /**
     * Appends a DataCapsule to the list of DataCapsules maintained by this
     * RecordCapsule.
     *
     * @param field The DataCapsule to add.
     * @param isKey Specifies if this DataCapsule is a key for this RecordCapsule
     * @return true if the DataCapsule is added.
     */
    public boolean addDataCapsule(DataCapsule field, boolean isKey) {
        boolean retval = false;
        if (field != null) {
            logger.debug("Adding datacapsule: " + field.getName() + " isKey:" + isKey);
            fields.add(field);
            if (isKey) {
                Object foo = field.getData();
                int type = java.sql.Types.OTHER;
                if (foo instanceof Integer || foo instanceof Double || foo instanceof Float) {
                    type = java.sql.Types.NUMERIC;
                } else if (foo instanceof java.sql.Date) {
                    type = java.sql.Types.DATE;
                } else if (foo instanceof java.sql.Timestamp) {
                    type = java.sql.Types.TIMESTAMP;
                } else if (foo instanceof String) {
                    type = java.sql.Types.CHAR;
                }
                KeySpecification kspec = new KeySpecification(field.getName(), fields.size() - 1, type);
                keys.add(kspec);
                // keys.add(new KeySpecification(fields.size() - 1, type));
            }
        }
        return retval;
    }

    /**
     * Returns the number of DataCapsules maintained by this RecordCapsule
     *
     * @return The number of DataCapsules
     */
    public int getFieldCount() {
        return fields.size();
    }

    /**
     * Returns a hash value for this RecordCapsule by accumulating the hash
     * values of the String representations of all of the DataCapsules.
     *
     * @return An integer hash
     */
    public int getKeyHash() {
        int retval = 0;
        for (int i = 0; i < keys.size(); i++) {
            retval += fields.get(i).getData().toString().hashCode();
        }
        return retval;
    }

    /*
     * @SuppressWarnings("unused") private void
     * setKey(ArrayList<KeySpecification> key) { this.keys = key; }
     */
    public boolean keyEquals(ArrayList<KeySpecification> newkey) {
        boolean retval = true;
        if (newkey != null && keys != null && keys.size() == newkey.size()) {
            for (int i = 0; i < keys.size(); i++) {
                retval = retval && keys.get(i).equals(newkey.get(i));
            }
        } else {
            retval = false;
        }

        return retval;
    }

    /**
     * Just a convenience method to allow the display of the contents of a
     * RecordCapsule for debugging purposes.
     *
     * @return The concatenated values of all DataCapsules in the format
     * "RecordCapsule:" RecordCapsule name < linefeed> DataCapsule name
     * <tab> DataCapsule contents (String representation)
     * <delimiter> <linefeed>.
     */
    public String toString() {
        StringBuffer sb = new StringBuffer();

        sb.append("RecordCapsule:" + name + "\n");
        Iterator<DataCapsule> it = fields.iterator();
        while (it.hasNext()) {
            DataCapsule foo = it.next();
            String name = foo.getName();
            while (name.length() < 20) {
                name = name + " ";
            }
            sb.append("  DataCapsule: " + name);
            if (foo.getData() != null) {
                if (foo.getData() instanceof Object[]) {
                    Object[] bar = (Object[]) foo.getData();
                    sb.append(AleppoConstants.ALEPPO_DELIMITER_DEFAULT + "[");
                    String separator = "";
                    for (Object aBar : bar) {
                        sb.append(separator + aBar.toString());
                        separator = ",";
                    }
                    sb.append("]");
                } else {
                    sb.append(AleppoConstants.ALEPPO_DELIMITER_DEFAULT + foo.getData().toString());
                }
            }
            sb.append("\n");
        }
        sb.append("\n");

        return sb.toString();
    }

    /**
     * This is a convenience item that can be used for substituting the value of
     * a DataCapsule into a string that contains the format %DC_NAME% where
     * DC_NAME is the name of a DataCapsule owned by this RecordCapsule. This is
     * useful when calling outside services, such as is done by RESTProcess for
     * example.
     *
     * @param input The String containing the tokens for substitution.
     * @return A String with the tokens substituted with the DataCapsule values.
     */
    public String replaceTokens(String input) {
        String retval = input;
        for (int i = 0; i < this.getFieldCount(); i++) {
            if (!this.getField(i).isNull) {
                String token = "%" + this.getField(i).getName().trim() + "%";
                if (retval.contains(token)) {
                    retval = retval.replace(token, this.getField(i).getData().toString());
                }
            }
        }
        return retval;
    }

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("RecordCapsule");

    protected ArrayList<KeySpecification> keys = new ArrayList<KeySpecification>();
    protected ArrayList<DataCapsule> fields = new ArrayList<DataCapsule>();
}
