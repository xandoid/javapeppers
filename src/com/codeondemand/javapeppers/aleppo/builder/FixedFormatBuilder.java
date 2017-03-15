/**
 *
 */
package com.codeondemand.javapeppers.aleppo.builder;

import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.FieldSpecification;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;
import org.apache.logging.log4j.LogManager;

import java.util.ArrayList;

/**
 * This class converts a RecordCapsule into a fixed format record using the
 * meta-data specified in the DataCapsule.
 *
 * @author gfa
 */
public class FixedFormatBuilder extends NullBuilder {

    public FixedFormatBuilder(String eol) {
        this.eol = eol;
    }

    public boolean addField(FieldSpecification field) {
        fields.add(field);
        return true;
    }

    public boolean doInitialization() {
        boolean retval = true;
        return retval;
    }

    private boolean buildFields(RecordCapsule r) {
        boolean retval = false;
        if (r != null && r.getFieldCount() > 0) {
            int start = 0;
            for (int i = 0; i < r.getFieldCount(); i++) {
                DataCapsule dc = r.getField(i);
                int length = 10;
                int type = java.sql.Types.VARCHAR;
                if (dc.getMetaData("length") != null) {
                    length = Integer.parseInt(dc.getMetaData("length").toString());
                }
                if (dc.getMetaData("type") != null) {
                    type = MiscUtil.getSQLType(dc.getMetaData("type").toString());
                }
                addField(new FieldSpecification(start, length, type, false));
            }
            retval = true;
        }
        return retval;
    }

    public Object buildRecord(RecordCapsule r) {

        if (!initialized) {
            initialized = buildFields(r);
        }
        String retval = null;
        StringBuffer sb = new StringBuffer();

        int idx = 0;
        while (idx < r.getFieldCount()) {
            DataCapsule foo = r.getField(idx);
            FieldSpecification fs = fields.get(idx++);
            String temp = foo.getData().toString();
            if (temp.length() > fs.getLength()) {
                temp = temp.substring(0, fs.getLength());
            } else {
                while (temp.length() < fs.getLength()) {
                    temp = temp + " "; //$NON-NLS-1$
                }
            }
            sb.append(temp);
        }
        sb.append(eol);
        retval = sb.toString();
        logger.debug("Record built: " + retval);
        return retval;
    }

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("FixedFormatBuilder");

    private boolean initialized = false;
    private String eol = "";
    private ArrayList<FieldSpecification> fields = new ArrayList<FieldSpecification>();
}
