/**
 *
 */
package com.codeondemand.javapeppers.aleppo.parser;

import com.codeondemand.javapeppers.aleppo.AleppoMessages;
import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.FieldSpecification;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.habanero.util.db.DbUtil;
import org.apache.logging.log4j.LogManager;

import java.sql.Types;
import java.util.Objects;

public class DelimiterParser extends KeyedRecordParser {

    public DelimiterParser(String delimiter) {
        if (delimiter != null && delimiter.length() > 0) {
            this.delimiter = delimiter;
            if (this.delimiter.equals("|")) {
                this.delimiter = DelimiterParser.PIPE_DELIMITER;
            }
        }
    }

    public RecordCapsule parseRecord(Object input) {
        RecordCapsule retval = null;

        logger.debug("Input record:" + input);
        // System.out.println( input);

        if (input instanceof String && ((String) input).length() > 0) {

            String[] tokens = null;


            if (Objects.equals(this.delimiter, DelimiterParser.COMMA_DELIMITER)) {
                tokens = ((String) input).split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            } else {
                tokens = ((String) input).split(delimiter, -1);
            }

            logger.debug(AleppoMessages.getString("DelimiterParser.0") + tokens.length); //$NON-NLS-1$

            if (tokens.length > 0) {
                int idx = 0;
                retval = new RecordCapsule("record", "record");
                Object data = null;
                if (fields.size() > 0) {
                    for (FieldSpecification field : fields) {
                        String temp = tokens[field.getField_num()];
                        data = null;
                        if (temp != null && temp.trim().length() > 0) {
                            switch (field.getType()) {
                                case Types.INTEGER:
                                    try {
                                        data = new Integer(temp);
                                    } catch (NumberFormatException nfe) {
                                        if (temp.startsWith(".")) {
                                            data = new Float(temp);
                                            field.setType(Types.FLOAT);
                                        }
                                    }
                                    break;
                                case Types.FLOAT:
                                case Types.REAL:
                                    data = new Float(temp);
                                    break;
                                // Convert decimal values to float so that we
                                // can normalize the format.
                                case Types.DECIMAL:
                                    data = new Float(temp);
                                    break;
                                case Types.TIME:
                                    data = DbUtil.parseDB2TimeString(temp);
                                    break;
                                case Types.TIMESTAMP:
                                    data = DbUtil.parseTimestampString(temp);
                                    break;
                                default:
                                    data = temp.trim();
                            }
                        }
                        String fld_name = field.getName();
                        if (!fld_name.startsWith("dummy")) {
                            DataCapsule bar = new DataCapsule(field.getName(), data);

                            retval.addDataCapsule(bar, field.isKey());

                            bar.setMetaData("isKey", isKeyField(field.getField_num()));
                            bar.setMetaData("type", field.getType());
                            bar.setMetaData("length", field.getLength());
                            bar.setMetaData("typeName", field.getTypeName());
                            bar.setMetaData("position", field.getField_num() + 1);
                            bar.setMetaData("isKey", field.isKey());

                            logger.debug(field.getType());
                            logger.debug(field.getTypeName());
                            logger.debug(field.getField_num());
                            logger.debug(field.getLength());
                            logger.debug(field.isKey());
                        }

                    }
                } else {
                    int fieldnum = 1;
                    while (idx < tokens.length) {

                        String token = tokens[idx];
                        if (token.length() == 0) {
                            token = null;
                        }
                        DataCapsule bar = new DataCapsule("FIELD" + fieldnum++, token);
                        retval.addDataCapsule(bar, isKeyField(idx++));
                    }
                }
            }
        }
        return retval;
    }

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("DelimiterParser");

    public static final String DEFAULT_DELIMITER = ","; //$NON-NLS-1$
    public static final String TAB_DELIMITER = "\\t"; //$NON-NLS-1$
    public static final String PIPE_DELIMITER = "\\|"; //$NON-NLS-1$
    public static final String COMMA_DELIMITER = ","; //$NON-NLS-1$

    private String delimiter = DEFAULT_DELIMITER;

}
