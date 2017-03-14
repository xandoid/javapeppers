/* 
 */
package com.codeondemand.javapeppers.aleppo.process;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Observer;
import java.util.Properties;
import java.util.TreeMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.logging.log4j.LogManager;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.codeondemand.javapeppers.aleppo.AleppoMessages;
import com.codeondemand.javapeppers.aleppo.builder.DDLBuilder;
import com.codeondemand.javapeppers.aleppo.builder.DelimiterBuilder;
import com.codeondemand.javapeppers.aleppo.builder.FixedFormatBuilder;
import com.codeondemand.javapeppers.aleppo.builder.RecordBuilder;
import com.codeondemand.javapeppers.aleppo.builder.SpecificationBuilder;
import com.codeondemand.javapeppers.aleppo.builder.XMLBuilder;
import com.codeondemand.javapeppers.aleppo.common.AleppoConstants;
import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.FlowNode;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordProcessor;
import com.codeondemand.javapeppers.aleppo.connector.RecordConnector;
import com.codeondemand.javapeppers.aleppo.destination.RecordDestination;
import com.codeondemand.javapeppers.aleppo.filter.RecordFilter;
import com.codeondemand.javapeppers.aleppo.monitor.MonitorProcess;
import com.codeondemand.javapeppers.aleppo.parser.JSONParser;
import com.codeondemand.javapeppers.aleppo.parser.NullRecordParser;
import com.codeondemand.javapeppers.aleppo.parser.ParserFactory;
import com.codeondemand.javapeppers.aleppo.parser.RecordParser;
import com.codeondemand.javapeppers.aleppo.reader.SourceReader;
import com.codeondemand.javapeppers.aleppo.reader.UISourceReader;
import com.codeondemand.javapeppers.aleppo.source.AggregateSource;
import com.codeondemand.javapeppers.aleppo.source.RecordSource;
import com.codeondemand.javapeppers.aleppo.source.SourceConcentrator;
import com.codeondemand.javapeppers.aleppo.transform.RecordTransform;
import com.codeondemand.javapeppers.aleppo.writer.DestinationWriter;
import com.codeondemand.javapeppers.aleppo.writer.FileRecordWriter;
import com.codeondemand.javapeppers.aleppo.writer.GZIPDataFileWriter;
import com.codeondemand.javapeppers.aleppo.writer.SplitFileWriter;
import com.codeondemand.javapeppers.aleppo.writer.UIRecordWriter;
import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;

public class ConfigurationLoader {

	public ArrayList<RecordConnector> initializeWithString(String configString) {
		ArrayList<RecordConnector> retval = new ArrayList<RecordConnector>();
		if (configString == null || configString.trim().length() == 0) {
			retval = null;
			logger.error(AleppoMessages.getString(AleppoMessages.getString("ConfigurationLoader.11"))); //$NON-NLS-1$
		} else {
			try {
				DocumentBuilder dbldr = DocumentBuilderFactory.newInstance().newDocumentBuilder();
				Document doc = dbldr.parse(new ByteArrayInputStream(configString.getBytes()));
				NodeList flowlist = doc.getElementsByTagName(AleppoConstants.ALEPPO_CONFIG_DATAFLOW_TAG);

				if (flowlist.getLength() != 1) {
					logger.error(AleppoMessages.getString("ConfigurationLoader.1")); //$NON-NLS-1$
					retval = null;
				} else {

					NodeList connlist = doc.getElementsByTagName(AleppoConstants.ALEPPO_CONFIG_CONNECTOR_TAG);
					for (int i = 0; i < connlist.getLength(); i++) {
						Element c = (Element) connlist.item(i);
						Attr a = c.getAttributeNode("instances");
						if (a != null) {
							int instances = new Integer(a.getValue()).intValue();
							for (int j = 0; j < instances; j++) {
								instance_num = j;
								retval.add(createConnector(c));
							}
						} else {
							retval.add(createConnector(c));
						}
					}
				}
			} catch (SAXException e) {
				retval = null;
				logger.error(e.toString());
			} catch (IOException e) {
				retval = null;
				logger.error(e.toString());
			} catch (ParserConfigurationException e) {
				retval = null;
				logger.error(e.toString());
			}
		}
		return retval;
	}

	/**
	 * Convenience method to load a file into a string and pass it on to the
	 * configuration loader method that takes a string as input.
	 * 
	 * @param configfile
	 *            The name of the configuration file. This will be loaded if it
	 *            can be located in the CLASSPATH.
	 * @return A configured and populated list of RecordConnector objects.
	 */
	public ArrayList<RecordConnector> initialize(String configfile) {
		ArrayList<RecordConnector> retval = new ArrayList<RecordConnector>();

		if (configfile == null || configfile.trim().length() == 0) {
			retval = null;
			logger.error(AleppoMessages.getString("ConfigurationLoader.0") + configfile); //$NON-NLS-1$
		} else {
			String config_data = MiscUtil.fileToString(configfile);
			retval = initializeWithString(config_data);

		}

		return retval;
	}

	/**
	 * This is the method that parses each of the source to destination flows
	 * that are defined in the dataflow configuration document.
	 * 
	 * @param n
	 *            An Element object that contains the document section that
	 *            defines the connector hierarchy
	 * 
	 * @return A RecordConeector object.
	 */
	private RecordConnector createConnector(Element n) {
		RecordConnector retval = null;

		// This is the class that was defined to provide the source to
		// destination data flow. Although it could be a custom
		// class, it is typically either one of these:
		// com.codeondemand.javapeppers.aleppo.connector.RecordConnector
		// com.codeondemand.javapeppers.aleppo.connector.MultiplexConnector.
		//
		String conn_class = getAttributeFromNode(n, AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_CLASS);

		// Instantiate the class.
		retval = (RecordConnector) createInstance(conn_class);

		if (retval != null) {

			// Now initialize the class using providing it with a TreeMap
			// of the parameters that are found for this element of the
			// configuration document.
			retval.initialize(getAttributes(n));

			// Get the properties object if it was created, so that is
			// is available to provide to other components as they are
			// initialized.
			props = retval.getProperties();
			if (props == null) {
				props = new Properties();
			}

			// The source for the dataflow will be one of three types
			// 1) An aggregator, meaning that data from multiple sources are
			// combined
			// at the beginning of the flow. The data need not be related, but
			// often is will be related by some key. This is somewhat like a
			// join
			// in a relational model.
			// 2) A concentrator, meaning that data from one source is
			// accumulated
			// for
			// a specific key and when the key changes, the record is allowed to
			// flow through the system.
			// 3) A single source (file,queue,query, UI, etc) that provides one
			// data
			// record to the data flow each time it is asked.
			//
			// There will always only be one of these types, never more than
			// one.

			// Check for aggregator. There may not be one, but that is fine.
			AggregateSource src_agg = null;
			NodeList aggsrc = n.getElementsByTagName(AleppoConstants.ALEPPO_CONFIG_AGGREGATOR_TAG);
			if (aggsrc.getLength() > 0) {
				Element agg = (Element) aggsrc.item(0);
				String aggClass = agg.getAttribute(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_CLASS);
				NodeList srcs = ((Element) agg).getElementsByTagName(AleppoConstants.ALEPPO_CONFIG_SOURCE_TAG);
				src_agg = createAggregator(aggClass, srcs);
			}

			// If there was no concentrator, then check for concentrator.
			// There may not be one, but that is also fine.
			SourceConcentrator src_conc = null;
			if (src_agg == null) {
				NodeList src = ((Element) n).getElementsByTagName(AleppoConstants.ALEPPO_CONFIG_CONCENTRATOR_TAG);
				if (src.getLength() > 0) {
					Element conc = (Element) src.item(0);
					String concClass = conc.getAttribute(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_CLASS);
					NodeList srcs = ((Element) conc).getElementsByTagName(AleppoConstants.ALEPPO_CONFIG_SOURCE_TAG);
					if (srcs.getLength() > 0) {
						src_conc = createConcentrator(concClass, createSource((Element) srcs.item(0)));
					}
				}
			}

			// Get the observers (clear out the global observer list for the other instances.
			flow_observers.clear();
			NodeList observerList = n.getElementsByTagName(AleppoConstants.ALEPPO_CONFIG_OBSERVERS_TAG);
			if (observerList.item(0) != null) {
				NodeList children = observerList.item(0).getChildNodes();
				for (int i = 0; i < children.getLength(); i++) {
					if( children.item(i).getNodeName().equals("observer")){
						Object temp = createObserver(children.item(i));
						flow_observers.add(temp);						
					}
				}
			}

			// If there has not been an aggregator or concentrator found, then
			// assume we
			// just have a simple source.
			RecordSource single_src = null;
			if (src_conc == null && src_agg == null) {
				logger.debug(AleppoMessages.getString("ConfigurationLoader.2")); //$NON-NLS-1$
				NodeList onesrc = n.getElementsByTagName(AleppoConstants.ALEPPO_CONFIG_SOURCE_TAG);

				// Note: As a part of the source creation, the reader and
				// parser
				// will also be created.
				if (onesrc.getLength() > 0) {
					single_src = createSource((Element) onesrc.item(0));
				}
			}

			// Get all of the RecordProcessor classes and instantiate them in
			// the
			// order that they are listed in the configuration file.
			NodeList proclist = n.getElementsByTagName(AleppoConstants.ALEPPO_CONFIG_PROCESS_TAG);
			if (proclist.item(0) != null) {
				NodeList children = proclist.item(0).getChildNodes();
				for (int i = 0; i < children.getLength(); i++) {
					RecordProcessor p = null;
					if (children.item(i).getNodeName().equals(AleppoConstants.ALEPPO_CONFIG_FILTER_TAG)) {
						p = createFilter(children.item(i));
					} else if (children.item(i).getNodeName().equals(AleppoConstants.ALEPPO_CONFIG_MONITOR_TAG)) {
						p = createMonitor(children.item(i));
					} else if (children.item(i).getNodeName().equals(AleppoConstants.ALEPPO_CONFIG_TRANSFORM_TAG)) {
						p = createTransform(children.item(i));
					} else if (children.item(i).getNodeName().equals(AleppoConstants.ALEPPO_CONFIG_ACTION_TAG)) {
						p = createProcess(children.item(i));
					}
					if (p != null) {
						retval.addProcess(p);
					}
				}
			}

			ArrayList<RecordDestination> dstlist = new ArrayList<RecordDestination>();
			NodeList dests = n.getElementsByTagName(AleppoConstants.ALEPPO_CONFIG_DESTINATION_TAG);
			for (int i = 0; i < dests.getLength(); i++) {
				Element d = (Element) dests.item(i);
				dstlist.add(createDestination(d));
				logger.debug(AleppoMessages.getString("ConfigurationLoader.3") + dstlist.get(i)); //$NON-NLS-1$
			}

			if (src_conc != null) {
				logger.debug(AleppoMessages.getString("ConfigurationLoader.4")); //$NON-NLS-1$
				retval.initialize(src_conc, dstlist.remove(0));
			} else if (single_src != null) {
				logger.debug(AleppoMessages.getString("ConfigurationLoader.5")); //$NON-NLS-1$
				retval.initialize(single_src, dstlist.remove(0));
			} else if (src_agg != null) {
				logger.debug(AleppoMessages.getString("ConfigurationLoader.6")); //$NON-NLS-1$
				retval.initialize(src_agg, dstlist.remove(0));
			} else {
				logger.debug(AleppoMessages.getString("ConfigurationLoader.7")); //$NON-NLS-1$
			}

			while (dstlist.size() > 0) {
				retval.addDestination(dstlist.remove(0));
			}
		}

		return retval;
	}

	private static SourceConcentrator createConcentrator(String name, RecordSource src) {
		SourceConcentrator retval = null;
		if (name != null && name.length() > 0) {
			logger.debug(AleppoMessages.getString("ConfigurationLoader.8") + name); //$NON-NLS-1$
			try {
				Object temp = Class.forName(name).newInstance();
				if (temp != null && temp instanceof SourceConcentrator) {
					retval = (SourceConcentrator) temp;
					((SourceConcentrator) retval).setSource(src);
				}
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return retval;
	}

	private AggregateSource createAggregator(String name, NodeList srcs) {
		AggregateSource retval = null;
		if (name != null && name.length() > 0) {
			logger.debug(AleppoMessages.getString("ConfigurationLoader.9") + name); //$NON-NLS-1$
			try {
				Object temp = Class.forName(name).newInstance();
				if (temp != null && temp instanceof AggregateSource) {
					retval = (AggregateSource) temp;
					logger.debug(AleppoMessages.getString("ConfigurationLoader.10") //$NON-NLS-1$
							+ srcs.getLength());
					for (int i = 0; i < srcs.getLength(); i++) {
						RecordSource src = createSource((Element) srcs.item(i));
						if (src != null) {
							((AggregateSource) retval).setSource(src);
						}
					}
				}
			} catch (InstantiationException e) {
				logger.error(e.toString());
			} catch (IllegalAccessException e) {
				logger.error(e.toString());
			} catch (ClassNotFoundException e) {
				logger.error(e.toString());
			}
		}

		return retval;
	}

	private RecordSource createSource(Element n) {
		RecordSource retval = null;
		String name = n.getAttribute("class"); //$NON-NLS-1$
		logger.debug(AleppoMessages.getString("ConfigurationLoader.12") + name); //$NON-NLS-1$

		if (name != null) {
			Object temp = null;
			try {
				temp = Class.forName(name).newInstance();
				if (temp instanceof RecordSource) {
					retval = (RecordSource) temp;

					// Build a source from a reader and a parser.
					SourceReader rdr = createReader(n.getElementsByTagName(AleppoConstants.ALEPPO_CONFIG_READER_TAG));
					RecordParser prsr = createParser(n.getElementsByTagName(AleppoConstants.ALEPPO_CONFIG_PARSER_TAG));
					if (rdr != null && prsr != null) {
						retval.initialize(rdr, prsr);
					} else {
						Object[] arg = new Object[1];
						if (getAttributeFromNode(n, AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_FILE) != null) {
							arg[0] = getAttributeFromNode(n, AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_FILE);
						}
						retval.initialize(arg);
					}
				} else {
					logger.error(AleppoMessages.getString("ConfigurationLoader.13")); //$NON-NLS-1$
				}
			} catch (InstantiationException e) {
				logger.error(e.toString());
			} catch (IllegalAccessException e) {
				logger.error(e.toString());
			} catch (ClassNotFoundException e) {
				logger.error(e.toString());
			}
		}

		return retval;
	}

	/**
	 * A parser is a class that takes in an object is some unspecified format
	 * and generates an output object is some other format. It is basically just
	 * a transformer in the context of an Aleppo dataflow, but it is a special
	 * case in that it is a component of a RecordSource.
	 * 
	 * @param nl
	 *            This is the NodeList object that is the Parser definition part
	 *            of the configuration document.
	 * @return An instance of a RecordParser object or null if one cannot be
	 *         instantiated
	 */
	private RecordParser createParser(NodeList nl) {
		RecordParser retval = null;
		if (nl != null) {

			String name = getAttributeFromNode(nl.item(0), "class"); //$NON-NLS-1$
			if (name != null) {

				logger.debug(AleppoMessages.getString("ConfigurationLoader.14") + name); //$NON-NLS-1$

				// There are currently only three formats of parsers that are
				// provided in the
				// Aleppo framework. These are a fixed format parser which
				// expects the format
				// to be specified in a separate file, a delimited format parser
				// which just
				// requires a delimiter string to be specified, and a null
				// parser which basically
				// just returns the incoming record unchanged.
				if (name.equals("com.codeondemand.javapeppers.aleppo.parser.FixedFormatParser")) { //$NON-NLS-1$
					retval = ParserFactory.createParser(ParserFactory.FIXED_FORMAT_PARSER,
							getAttributeFromNode(nl.item(0), "file")); //$NON-NLS-1$
				} else if (name.equals("com.codeondemand.javapeppers.aleppo.parser.DelimiterParser")) { //$NON-NLS-1$
					String filename = getAttributeFromNode(nl.item(0), "file"); //$NON-NLS-1$
					String delimiter = getAttributeFromNode(nl.item(0), "delimiter"); //$NON-NLS-1$

					if (filename == null || filename.length() == 0) {
						logger.debug(AleppoMessages.getString("ConfigurationLoader.20")); //$NON-NLS-1$
						String args = delimiter;
						retval = ParserFactory.createParser(ParserFactory.DELIMITED_PARSER, args);
					} else {
						String[] args = { delimiter, filename };
						retval = ParserFactory.createParser(ParserFactory.DELIMITED_PARSER, args);
					}
				} else if (name.equals("com.codeondemand.javapeppers.aleppo.parser.NullRecordParser")) { //$NON-NLS-1$
					NullRecordParser p = new NullRecordParser();
					retval = p;
				} else if (name.equals("com.codeondemand.javapeppers.aleppo.parser.JSONParser")) { //$NON-NLS-1$
					JSONParser p = new JSONParser();
					retval = p;
					// Set the properties object
					retval.setProperties(props);

					// Initialize based on some application specific data that
					// is passed in the document structure below the reader.
					retval.initialize(getAttributes(nl.item(0)));
					
				}
			}

			// Set the global properties for this parser to use.
			if (retval != null) {
				retval.setProperties(props);
				logger.debug(AleppoMessages.getString("ConfigurationLoader.22") + retval.toString()); //$NON-NLS-1$
			} else {
				logger.error(AleppoMessages.getString("ConfigurationLoader.23")); //$NON-NLS-1$
			}
		}
		return retval;
	}

	/**
	 * This method attempts to instantiate an object which will read the next
	 * record from the incoming data source.
	 * 
	 * @param nl
	 *            The NodeList object containing the reader definition part of
	 *            the configuration file.
	 * @return An instance of a SourceReader object, or a null if one cannot be
	 *         instantiated.
	 */
	private SourceReader createReader(NodeList nl) {
		SourceReader retval = null;
		if (nl != null) {
			String name = getAttributeFromNode(nl.item(0), "class"); //$NON-NLS-1$

			if (name != null) {
				logger.debug(AleppoMessages.getString("ConfigurationLoader.25") + name); //$NON-NLS-1$
				retval = (SourceReader) createInstance(name);

				if (retval != null) {
					logger.debug(AleppoMessages.getString("ConfigurationLoader.26") + name + " " //$NON-NLS-1$ //$NON-NLS-2$
							+ retval.toString());

					// Set the properties object
					retval.setProperties(props);

					// Initialize based on some application specific data that
					// is passed in the document structure below the reader.
					retval.initialize(getAttributes(nl.item(0)));

					// Handle some special case for a UI data source.
					if (retval instanceof UISourceReader) {
						NodeList r = ((Element) nl.item(0))
								.getElementsByTagName(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_RECORD);
						if (r.getLength() > 0) {
							RecordCapsule rc = buildRecordCapsule((Element) r.item(0));
							((UISourceReader) retval).initialize(rc);
						}
					}

				} else {
					logger.error(AleppoMessages.getString("ConfigurationLoader.28") + name); //$NON-NLS-1$
				}
			} else {
				logger.error(AleppoMessages.getString("ConfigurationLoader.15")); //$NON-NLS-1$
			}
		}
		return retval;
	}

	/**
	 * A Monitor is pretty much like any other processing node. There is an
	 * expectation that it will always return the input record after it takes
	 * what information it needs.
	 * 
	 * @param n
	 *            A Document node containing the information to configure the
	 *            monitor.
	 * 
	 * @return An instantiated and initialized MonitorProcess or null
	 */
	private MonitorProcess createMonitor(Node n) {
		MonitorProcess retval = null;
		RecordProcessor temp = createProcess(n);
		if (temp instanceof MonitorProcess) {
			retval = (MonitorProcess) temp;
		} else {
			logger.error(AleppoMessages.getString("ConfigurationLoader.29")); //$NON-NLS-1$
		}

		return retval;
	}

	/**
	 * Creates an Observer object
	 * 
	 * @param n
	 *            The portion of the Document specifying the Filter object
	 * @return An Object or null.
	 */
	private Object createObserver(Node n) {
		Object retval = null;
		TreeMap<String, Object> attmap = getAttributes(n);
		String name = (String) attmap.get(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_CLASS);
		if (name != null) {
			logger.debug(AleppoMessages.getString("ConfigurationLoader.32") + name); //$NON-NLS-1$
			Object temp;
			temp = createInstance(name);
			if (temp != null) {
				try {
					temp.getClass().getMethod("isObservable");
					if (attmap != null) {
						((FlowNode) temp).initialize(getAttributes(n));
					}
					((FlowNode) temp).setProperties(props);
					retval = temp;
					// System.out.println( temp.getClass());
					logger.debug(AleppoMessages.getString("RecordConnector.16") + temp); //$NON-NLS-1$
				} catch (NoSuchMethodException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (SecurityException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			retval = temp;
		}
		return retval;
	}

	/**
	 * Creates a RecordFilter object if it is properly specified. A RecordFilter
	 * may or may not return the incoming record.
	 * 
	 * @param n
	 *            The portion of the Document specifying the Filter object
	 * @return A RecordFilter or null.
	 */
	private RecordFilter createFilter(Node n) {
		RecordFilter retval = null;
		Object temp = createProcess(n);
		if (temp instanceof RecordFilter) {
			retval = (RecordFilter) temp;
		} else {
			logger.error(AleppoMessages.getString("ConfigurationLoader.30")); //$NON-NLS-1$
		}
		return retval;
	}

	// Convenience method to allow type checking of a RecordTransform
	private RecordTransform createTransform(Node n) {
		RecordTransform retval = null;
		RecordProcessor temp = createProcess(n);
		if (temp instanceof RecordTransform) {
			retval = (RecordTransform) temp;
		} else {
			logger.error(AleppoMessages.getString("ConfigurationLoader.31")); //$NON-NLS-1$
		}

		return retval;
	}

	private RecordDestination createDestination(Element d) {
		RecordDestination retval = null;

		// Build a destination from a builder and a writer. The builder will
		// be responsible for formatting the output object in a suitable
		// manner so that the writer can output it to the final form.
		RecordBuilder bldr = createBuilder(d.getElementsByTagName(AleppoConstants.ALEPPO_CONFIG_BUILDER_TAG));
		DestinationWriter wrtr = createWriter(d.getElementsByTagName(AleppoConstants.ALEPPO_CONFIG_WRITER_TAG));

		RecordDestination dst = new RecordDestination();

		wrtr.setProperties(props);
		dst.initialize(wrtr, bldr);
		String maxrec = d.getAttribute(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_MAXROWS);
		if (maxrec != null && maxrec.length() > 0) {
			dst.setMaxRecordCount(new Long(maxrec).longValue());
		}
		retval = dst;
		return retval;
	}

	private RecordProcessor createProcess(Node n) {
		RecordProcessor retval = null;

		TreeMap<String, Object> attmap = getAttributes(n);
		String name = (String) attmap.get(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_CLASS);
		if (name != null) {
			logger.debug(AleppoMessages.getString("ConfigurationLoader.32") + name); //$NON-NLS-1$

			Object temp;
			temp = createInstance(name);

			logger.debug(AleppoMessages.getString("ConfigurationLoader.33") + temp.toString()); //$NON-NLS-1$

			if (temp instanceof RecordProcessor) {
				if (flow_observers.size() > 0) {
					for (int i = 0; i < flow_observers.size(); i++) {
						Object flow_observer = flow_observers.get(i);
						// System.out.println( "Adding observer:
						// "+flow_observer.getClass().toGenericString()+ " to "+
						// temp.getClass().toGenericString() );
						((RecordProcessor) temp).addObserver((Observer) flow_observer);
					}
				}
				retval = (RecordProcessor) temp;
				retval.setProperties(props);
				NodeList r = ((Element) n).getElementsByTagName(AleppoConstants.ALEPPO_CONFIG_RECORD_TAG);
				if (r.getLength() > 0) {
					RecordCapsule rc = buildRecordCapsule((Element) r.item(0));
					retval.setProcessData(rc);
				}
				if (attmap != null) {
					retval.initialize(getAttributes(n));
				}
			} else {
				logger.error(AleppoMessages.getString("ConfigurationLoader.34")); //$NON-NLS-1$
			}
		}

		return retval;
	}

	private DestinationWriter createWriter(NodeList nl) {
		DestinationWriter retval = null;
		String name = getAttributeFromNode(nl.item(0), "class"); //$NON-NLS-1$
		if (name != null) {
			if (name.equals("com.codeondemand.javapeppers.aleppo.writer.FileRecordWriter")) { //$NON-NLS-1$
				retval = new FileRecordWriter();
				((FileRecordWriter) retval).setPmap(getAttributes(nl.item(0)));
				((FileRecordWriter) retval).setProperties(props);

				((FileRecordWriter) retval).initialize(getAttributeFromNode(nl.item(0), "file"), Boolean //$NON-NLS-1$
						.parseBoolean(getAttributeFromNode(nl.item(0), "append"))); //$NON-NLS-1$
			} else if (name.equals("com.codeondemand.javapeppers.aleppo.writer.SplitFileWriter")) { //$NON-NLS-1$
				retval = new SplitFileWriter();
				((SplitFileWriter) retval).setPmap(getAttributes(nl.item(0)));
				((SplitFileWriter) retval).setProperties(props);

				((SplitFileWriter) retval).initialize(getAttributeFromNode(nl.item(0), "file"), Boolean //$NON-NLS-1$
						.parseBoolean(getAttributeFromNode(nl.item(0), "append"))); //$NON-NLS-1$
			} else if (name.equals("com.codeondemand.javapeppers.aleppo.writer.GZIPDataFileWriter")) { //$NON-NLS-1$
				retval = new GZIPDataFileWriter();
				((GZIPDataFileWriter) retval).setPmap(getAttributes(nl.item(0)));
				((GZIPDataFileWriter) retval).setProperties(props);

				((GZIPDataFileWriter) retval).initialize(getAttributeFromNode(nl.item(0), "file"), Boolean //$NON-NLS-1$
						.parseBoolean(getAttributeFromNode(nl.item(0), "append"))); //$NON-NLS-1$
			} else {
				retval = (DestinationWriter) createInstance(name);

				if (retval == null) {
					logger.error(AleppoMessages.getString("ConfigurationLoader.16") + name); //$NON-NLS-1$
				} else {
					retval.setProperties(props);
					if (retval instanceof UIRecordWriter) {
						NodeList r = ((Element) nl.item(0)).getElementsByTagName("record"); //$NON-NLS-1$
						if (r.getLength() > 0) {
							RecordCapsule rc = buildRecordCapsule((Element) r.item(0));
							((UIRecordWriter) retval).initialize(rc);
						}
					} else if (retval instanceof DestinationWriter) {
						((DestinationWriter) retval).setPmap(getAttributes(nl.item(0)));
					}
				}
			}
		}

		return retval;
	}

	/**
	 * Creates a record builder object. This is basically a formatting operation
	 * that creates the output object that the writer processes.
	 * 
	 * @param nl
	 *            The document section that specifies the builder
	 * @return A RecordBuilder instance or null.
	 */
	private RecordBuilder createBuilder(NodeList nl) {
		RecordBuilder retval = null;
		String name = getAttributeFromNode(nl.item(0), "class"); //$NON-NLS-1$
		logger.debug(AleppoMessages.getString("ConfigurationLoader.41") + name); //$NON-NLS-1$
		if (name != null) {
			if (name.equals("com.codeondemand.javapeppers.aleppo.builder.DelimiterBuilder")) { //$NON-NLS-1$
				retval = new DelimiterBuilder(getAttributeFromNode(nl.item(0), "delimiter")); //$NON-NLS-1$
			} else if (name.equals("com.codeondemand.javapeppers.aleppo.builder.SpecificationBuilder")) { //$NON-NLS-1$
				retval = new SpecificationBuilder(getAttributeFromNode(nl.item(0), "delimiter")); //$NON-NLS-1$
			} else if (name.equals("com.codeondemand.javapeppers.aleppo.builder.DDLBuilder")) { //$NON-NLS-1$
				retval = new DDLBuilder(getAttributeFromNode(nl.item(0), "schema"), //$NON-NLS-1$
						getAttributeFromNode(nl.item(0), "table")); //$NON-NLS-1$
			} else if (name.equals("com.codeondemand.javapeppers.aleppo.builder.FixedFormatBuilder")) { //$NON-NLS-1$
				retval = new FixedFormatBuilder(getAttributeFromNode(nl.item(0), "eol")); // $NON-NLS-2$
			} else if (name.equals("com.codeondemand.javapeppers.aleppo.builder.XMLBuilder")) { //$NON-NLS-1$
				retval = new XMLBuilder(getAttributeFromNode(nl.item(0), "rootname").toString(), //$NON-NLS-1$
						Boolean.parseBoolean(getAttributeFromNode(nl.item(0), "dopreamble").toString())); //$NON-NLS-1$
			} else {
				retval = (RecordBuilder) createInstance(name);
			}

			((RecordBuilder) retval).initialize(getAttributes(nl.item(0)));
			((RecordBuilder) retval).setProperties(props);
		}
		if (retval == null) {
			logger.error(AleppoMessages.getString("ConfigurationLoader.18") //$NON-NLS-1$
					+ name);
		} else {
			logger.debug("Instantiated builder:" + retval); //$NON-NLS-1$
			retval.setProperties(props);
		}
		return retval;
	}

	// Convenience method for creating an aleppo processing object. Allows
	// centralized debugging and error logging.
	private static Object createInstance(String name) {
		logger.debug(AleppoMessages.getString("ConfigurationLoader.53") + name); //$NON-NLS-1$
		Object retval = null;
		try {
			retval = Class.forName(name).newInstance();
		} catch (InstantiationException e) {
			logger.error(e.toString());
		} catch (IllegalAccessException e) {
			logger.error(e.toString());
		} catch (ClassNotFoundException e) {
			logger.error(e.toString());
		}
		return retval;
	}

	private static TreeMap<String, Object> getAttributes(Node nl) {
		TreeMap<String, Object> map = new TreeMap<String, Object>();
		NamedNodeMap attmap = nl.getAttributes();
		for (int i = 0; i < attmap.getLength(); i++) {
			String name = attmap.item(i).getNodeName();
			String value = attmap.item(i).getNodeValue();
			value = value.replace("@I_NUM@", getInstanceNumber(instance_num));
			value = value.replace("@I_CHAR@", getInstanceString(instance_num));
			map.put(name, value);
			// map.put(attmap.item(i).getNodeName(),
			// attmap.item(i).getNodeValue());
			logger.debug(AleppoMessages.getString("ConfigurationLoader.54") + attmap.item(i).getNodeName() //$NON-NLS-1$
					+ AleppoMessages.getString("ConfigurationLoader.55") + attmap.item(i).getNodeValue()); //$NON-NLS-1$
		}
		return map;
	}

	private static String getInstanceNumber(int index){
		String retval = "";
		if( index < 10){
			retval = "0"+index;
		}else{
			retval = retval+index;
		}
		return retval;
	}
	private static String getInstanceString(int index) {
		String retval = "";
		String foo = "abcdefghijklmnopqrtuvwxyz";
		if (index < foo.length()) {
			retval = foo.substring(index, index + 1);
		} else {
			int idx0 = (int) (foo.length() / index);
			int idx1 = index % foo.length();
			int idx2 = index;
			retval = foo.substring(idx0, idx0 + 1);
			while (idx1 + foo.length() <= idx2) {
				retval = retval + foo.substring(idx1, idx1 + 1);
				idx1 = idx1 + 1;
				idx2 = idx2 - foo.length();
			}
		}
		return retval;
	}

	private static String getAttributeFromNode(Node nl, String name) {
		String retval = null;
		if (nl != null) {
			NamedNodeMap attmap = nl.getAttributes();
			Node n = attmap.getNamedItem(name);
			if (n != null) {
				retval = attmap.getNamedItem(name).getNodeValue();
				retval = retval.replace("@I_NUM@", getInstanceNumber(instance_num));
				retval = retval.replace("@I_CHAR@", getInstanceString(instance_num));
			} else {
				logger.error(AleppoMessages.getString("ConfigurationLoader.56") + name); //$NON-NLS-1$
				retval = ""; //$NON-NLS-1$
			}
		}
		return retval;
	}

	private static RecordCapsule buildRecordCapsule(Element n) {

		RecordCapsule retval = null;
		NamedNodeMap m = n.getAttributes();
		if (m.getNamedItem("name") != null) { //$NON-NLS-1$
			retval = new RecordCapsule(n.getAttribute("name"), null); //$NON-NLS-1$
			for (int j = 0; j < m.getLength(); j++) {
				logger.debug(m.item(j).getNodeName() + ":" //$NON-NLS-1$
						+ m.item(j).getNodeValue());
				if (m.item(j).getNodeName() != "name") { //$NON-NLS-1$
					retval.setMetaData(m.item(j).getNodeName(), m.item(j).getNodeValue());
				}
			}
			NodeList fields = n.getElementsByTagName(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_FIELD);
			for (int i = 0; i < fields.getLength(); i++) {
				Element f = (Element) fields.item(i);
				m = f.getAttributes();
				if (m.getNamedItem(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_NAME) != null) {
					DataCapsule dc = new DataCapsule(
							m.getNamedItem(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_NAME).getNodeValue(), null);
					for (int j = 0; j < m.getLength(); j++) {
						logger.debug(m.item(j).getNodeName() + ":" //$NON-NLS-1$
								+ m.item(j).getNodeValue());
						if (m.item(j).getNodeName() != AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_NAME) {
							dc.setMetaData(m.item(j).getNodeName(), m.item(j).getNodeValue());
						}
					}
					retval.addDataCapsule(dc,
							new Boolean(f.getAttribute(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_ISKEY)).booleanValue());
				}
			}
		}
		return retval;
	}

	private ArrayList<Object> flow_observers = new ArrayList<Object>();
	private Properties props = null;
	private static int instance_num = 0;

	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("ConfigurationLoader");
}