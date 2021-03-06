/**
 *
 */
package com.codeondemand.javapeppers.aleppo.parser;

import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.FieldSpecification;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import org.apache.logging.log4j.LogManager;

import java.sql.Types;

public class FixedFormatParser extends KeyedRecordParser {

    public boolean addField(FieldSpecification field) {
        fields.add(field);
        return true;
    }

    public RecordCapsule parseRecord(Object input) {
        RecordCapsule r = null;
        if (input instanceof String) {
            r = new RecordCapsule("record", "record"); // NON-NLS-1
            //int idx = 0;
            for (FieldSpecification fs : fields) {
                String temp = null;
                logger.debug(fs.getStart_pos() + ":" + fs.getLength() + ":" + fs.getType());
                // Parse the fixed length section of the record and trim any
                // leading or trailing blanks.
                Object data = null;
                if (((String) input).length() > fs.getStart_pos() - 1 + fs.getLength()) {
                    temp = ((String) input).substring(fs.getStart_pos() - 1, fs.getStart_pos() - 1 + fs.getLength()).trim();
                    if (temp.length() > 0) {
                        switch (fs.getType()) {

                            case Types.INTEGER:
                                try {
                                    data = new Float(temp).intValue();
                                } catch (NumberFormatException nfe) {
                                    if (temp.trim().equals(".")) {
                                        logger.debug("Treating . as null entry");
                                    } else {
                                        logger.error(nfe.toString());
                                    }
                                }
                                break;
                            // Convert decimal values to float so that we
                            //	can normalize the format.
                            case Types.DECIMAL:
                                data = new Float(temp);
                                break;
                            default:
                                data = temp;
                        }
                    }
                }
                DataCapsule foo = new DataCapsule(fs.getName(), data);
                foo.setMetaData("type", fs.getType());
                foo.setMetaData("length", fs.getLength());
                foo.setMetaData("typeName", fs.getTypeName());
                foo.setMetaData("position", fs.getField_num() + 1);
                foo.setMetaData("isKey", fs.isKey());
                r.addDataCapsule(foo, fs.isKey());
            }

        }
        return r;
    }

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("FixedFormatParser");

}
