/**
 * 
 */
package com.codeondemand.javapeppers.aleppo.parser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;

import com.codeondemand.javapeppers.aleppo.AleppoMessages;
import com.codeondemand.javapeppers.aleppo.common.FieldSpecification;
import com.codeondemand.javapeppers.aleppo.common.KeySpecification;
import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;

public class ParserFactory {

	public static RecordParser createParser(int type, Object args) {
		RecordParser retval = null;

		switch (type) {
		case FIXED_FORMAT_PARSER:
			if (args instanceof String) {
				retval = createFixedFormatParser((String) args);
			}
			break;
		case DELIMITED_PARSER:
			if( args instanceof String[] && ((String[])args).length == 2 ){
				retval = createDelimiterParser( ((String[])args)[0], ((String[])args)[1]);
			}else if( args instanceof String ){
				retval = createDelimiterParser( (String)args,null);
			}
			break;
		case XMLNODE_PARSER:
			break;
		default:
			logger.error(AleppoMessages.getString("ParserFactory.0")); //$NON-NLS-1$
		}
		return retval;
	}

	
	private static RecordParser createDelimiterParser(String delimiter,String formatfile) {

		logger.debug(AleppoMessages.getString("ParserFactory.1") + delimiter+ " "+ formatfile); //$NON-NLS-1$ //$NON-NLS-2$
		DelimiterParser retval =  new DelimiterParser(delimiter);
		
		if( formatfile != null ){
			addFieldSpecifications( retval, formatfile);
		}
		return retval;
	}

	private static FixedFormatParser createFixedFormatParser(String formatfile) {
		FixedFormatParser retval = new FixedFormatParser();

		addFieldSpecifications( retval, formatfile );
		
		return retval;
	}

	private static boolean addFieldSpecifications( KeyedRecordParser p , String formatfile ){
		boolean retval = true;
		if( formatfile != null ){
			BufferedReader brdr;
			ArrayList<String[]> list = new ArrayList<String[]>();

			try {

				logger.debug(AleppoMessages.getString("ParserFactory.3") + formatfile); //$NON-NLS-1$

				brdr = new BufferedReader(new FileReader(new File(formatfile)));

				// read all of the lines in the file and process
                //int order = 0;
				while (brdr.ready()) {
					String line = brdr.readLine();
					if (line != null && line.trim().length() > 0) {
						logger.debug(line);
						// remove quotes that are coming from the format file
						// if any are present.
						line = line.replace('"', ' ').trim();
						String[] tokens = line.trim().split(",", -1); //$NON-NLS-1$
						if ( tokens != null && tokens.length > 2) {
							list.add(tokens);							
						}
					}
				}
				logger.debug(AleppoMessages.getString("ParserFactory.5") + list.size() //$NON-NLS-1$
						+ AleppoMessages.getString("ParserFactory.6")); //$NON-NLS-1$

				for( int item = 0 ; item < list.size(); item++){
					
					String[] tokens = list.get(item);
					// Pull in the basic fields, name, type and isKey
					String name = tokens[0].trim();
					String strType = tokens[1].trim();
					int type = MiscUtil.getSQLType(strType);				
					boolean isKey = Boolean.parseBoolean(tokens[2]);
					
					// If there are more tokens, then pull in the 
					// starting position and length for fixed format things.
					int start = -1;
					int len = -1;
					if( tokens.length == 5){
						start = Integer.parseInt(tokens[3]);
						len = Integer.parseInt(tokens[4]);						
					}
					
					// Build the fields spec and add to the parser
					FieldSpecification fs = new FieldSpecification(start, len, type);
					fs.setKey(isKey);
					fs.setName(name);
					fs.setTypeName(strType);
					fs.setField_num(item);
					
					p.addField(fs);					
					if (isKey ) {
						KeySpecification ks = new KeySpecification(item, type);
						p.addKeySpecification(ks);
					}
					
					// Debug message
					logger.debug(name + ":" + type + ":" + isKey + ":" + start + ":" //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
							 + len);
				}

			} catch (FileNotFoundException e) {
				logger.error(AleppoMessages.getString("ParserFactory.11")); //$NON-NLS-1$
				System.exit(1);
			} catch (IOException e) {
				logger.error(AleppoMessages.getString("ParserFactory.12") //$NON-NLS-1$
						+ e.toString());
				System.exit(1);
			}

		}
		return retval;
	}
	
	public static final int FIXED_FORMAT_PARSER = 1;
	public static final int DELIMITED_PARSER = 2;
	public static final int XMLNODE_PARSER   = 3;
	public static final int NULL_PARSER      = 4;
	public static final int JSON_PARSER = 5;

	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("ParserFactory");
}
