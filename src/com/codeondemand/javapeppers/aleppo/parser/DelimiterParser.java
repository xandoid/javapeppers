/**
 * 
 */
package com.codeondemand.javapeppers.aleppo.parser;

import org.apache.logging.log4j.LogManager;

import com.codeondemand.javapeppers.aleppo.AleppoMessages;
import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.FieldSpecification;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.habanero.util.db.DbUtil;

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
			
			
			if (this.delimiter == DelimiterParser.COMMA_DELIMITER) {
				tokens = ((String)input).split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
			} else {
				tokens = ((String) input).split(delimiter, -1);
			}

			logger.debug(AleppoMessages.getString("DelimiterParser.0") + tokens.length); //$NON-NLS-1$

			if (tokens.length > 0) {
				int idx = 0;
				retval = new RecordCapsule("record", "record");
				Object data = null;
				if (fields.size() > 0) {
					for (int i = 0; i < fields.size(); i++) {
						String temp = tokens[fields.get(i).getField_num()];
						data = null;
						if (temp != null && temp.trim().length() > 0) {
							switch (fields.get(i).getType()) {
							case java.sql.Types.INTEGER:
								try {
									data = new Integer(temp);
								} catch (NumberFormatException nfe) {
									if (temp.startsWith(".")) {
										data = new Float(temp);
										fields.get(i).setType(java.sql.Types.FLOAT);
									}
								}
								break;
							case java.sql.Types.FLOAT:
							case java.sql.Types.REAL:
								data = new Float(temp);
								break;
							// Convert decimal values to float so that we
							// can normalize the format.
							case java.sql.Types.DECIMAL:
								data = new Float(temp);
								break;
							case java.sql.Types.TIME:
								data = DbUtil.parseDB2TimeString(temp);
								break;
							case java.sql.Types.TIMESTAMP:
								data = DbUtil.parseTimestampString(temp);
								break;
							default:
								data = temp.trim();
							}
						}
						String fld_name = fields.get(i).getName();
						if (!fld_name.startsWith("dummy")) {
							DataCapsule bar = new DataCapsule(fields.get(i).getName(), data);

							retval.addDataCapsule(bar, fields.get(i).isKey());

							FieldSpecification fs = fields.get(i);
							bar.setMetaData("isKey", isKeyField(fields.get(i).getField_num()));
							bar.setMetaData("type", fs.getType());
							bar.setMetaData("length", fs.getLength());
							bar.setMetaData("typeName", fs.getTypeName());
							bar.setMetaData("position", fs.getField_num() + 1);
							bar.setMetaData("isKey", fs.isKey());

							logger.debug(fs.getType());
							logger.debug(fs.getTypeName());
							logger.debug(fs.getField_num());
							logger.debug(fs.getLength());
							logger.debug(fs.isKey());
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
