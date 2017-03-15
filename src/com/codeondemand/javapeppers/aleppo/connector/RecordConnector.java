package com.codeondemand.javapeppers.aleppo.connector;

import com.codeondemand.javapeppers.aleppo.AleppoMessages;
import com.codeondemand.javapeppers.aleppo.common.AleppoConstants;
import com.codeondemand.javapeppers.aleppo.common.FlowNode;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordProcessor;
import com.codeondemand.javapeppers.aleppo.destination.RecordDestination;
import com.codeondemand.javapeppers.aleppo.filter.RecordFilter;
import com.codeondemand.javapeppers.aleppo.source.AbstractRecordSource;
import com.codeondemand.javapeppers.aleppo.source.RecordSource;
import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;
import org.apache.logging.log4j.LogManager;

import java.util.*;

/**
 * The RecordConnector class provides the functionality to move data from a
 * RecordSource to a Record Destination with optional calls through a series of
 * processing nodes which may provide filtering and/or transformations services.
 * A RecordConnector implements the Runnable interface so that it can be
 * instantiated on an independent Thread for its processing activity.
 *
 * @author gfa
 */
public class RecordConnector extends FlowNode implements Runnable {

    /**
     * This method will call the call the close method for both the source and
     * destination components. Since processing nodes such as filters and
     * transforms are intended to be independent, they need not be closed.
     *
     * @return Returns true if the source and destination were both closed.
     */
    public boolean close() {
        boolean retval = true;

        moved = 0L;
        if (src != null) {
            retval = src.closeSource() && retval;
        }
        if (dst != null) {
            for (int i = 0; i < dst.size(); i++) {
                retval = dst.elementAt(i).closeDestination() && retval;
            }
        }
        for (int i = 0; i < processors.size(); i++) {
            processors.elementAt(i).done();
        }
        return retval;
    }

    /**
     * Adds a destination to the connector.  Note that a processor can
     * have multiple destinations for the records.
     *
     * @param destination A RecordDestination to handle processed records.
     * @return true if a RecordDestination is successfully addded.
     */
    public boolean addDestination(RecordDestination destination) {
        boolean retval = false;
        if (destination != null) {
            dst.add(destination);
            retval = true;
        } else {
            logger.debug(AleppoMessages.getString("RecordConnector.19")); //$NON-NLS-1$
        }
        return retval;
    }

    public boolean initialize(AbstractRecordSource source, RecordDestination destination) {
        boolean retval = false;

        if (source != null && destination != null && src == null && dst.size() == 0) {
            src = source;
            if (props != null) {
                src.setProperties(props);
            }
            dst.add(destination);
            logger.debug(this.toString() + AleppoMessages.getString("RecordConnector.0") //$NON-NLS-1$
                    + src.toString() + AleppoMessages.getString("RecordConnector.1") + destination.toString()); //$NON-NLS-1$
            retval = true;
        } else {
            logger.debug(AleppoMessages.getString("RecordConnector.5"));                //$NON-NLS-1$
            logger.debug(AleppoMessages.getString("RecordConnector.6") + source +       //$NON-NLS-1$
                    AleppoMessages.getString("RecordConnector.7") + destination);  //$NON-NLS-1$

        }
        return retval;
    }

    /**
     * An initialization method that takes a single source, destination, and
     * processor component.
     *
     * @param source      A RecordSource component for getting the records.
     * @param destination A RecordDestination component for putting the records.
     * @param processor   An optional RecordProcessor component such as a filter.
     * @return true if successfully initialized.
     */
    public boolean initialize(RecordSource source, RecordDestination destination, RecordProcessor processor) {

        boolean retval = false;
        if (processor != null) {
            RecordProcessor[] foo = {processor};
            retval = initialize(source, destination, foo);
        } else {
            retval = initialize(source, destination);
        }

        return retval;
    }

    /**
     * Allows initialization of a RecordConnector with multiple processing
     * nodes.
     *
     * @param src   The record source component.
     * @param dest  The record destination component.
     * @param procs An array of processing node components such as filters.
     * @return true if the RecordConnector is successfully initialized
     */
    public boolean initialize(RecordSource src, RecordDestination dest, RecordProcessor[] procs) {
        boolean retval = true;

        if (src != null && dest != null) {
            retval = initialize(src, dest);
        }

        if (procs != null && procs.length > 0) {
            for (RecordProcessor proc : procs) {
                if (proc != null && proc instanceof RecordProcessor) {
                    logger.debug(AleppoMessages.getString("RecordConnector.2") + proc.getClass()); //$NON-NLS-1$
                    processors.add((RecordProcessor) proc);

                } else {
                    retval = false;
                    processors.clear();
                    break;
                }
            }
        }
        return retval;
    }

    /**
     * Moves a single record between a record source and a record destination.
     *
     * @return true if there was a record to move.
     */
    public boolean moveRecord() {
        boolean retval = false;

        logger.debug(AleppoMessages.getString("RecordConnector.8") + src); //$NON-NLS-1$

        if (src != null) {

            // Get the record from the source
            RecordCapsule recin = src.getNextRecord();
            logger.debug(AleppoMessages.getString("RecordConnector.9") + recin); //$NON-NLS-1$

            // If there is a record, then process it and push it to
            // the destination
            if (recin != null) {
                // Add global properties specified for the connector.
                if (props != null) {
                    addGlobalProperties(recin);
                }
                retval = true;
                if (processors != null && processors.size() > 0) {
                    for (int i = 0; i < processors.size(); i++) {
                        logger.debug(AleppoMessages.getString("RecordConnector.10") + processors.elementAt(i)); //$NON-NLS-1$
                        try {
                            recin = processors.elementAt(i).processRecord(recin);
                        } catch (Exception e) {
                            logger.error(e.toString());
                            e.printStackTrace();
                            recin = null;
                            retval = false;
                            break;
                        }
                        if (recin == null) {
                            logger.debug(AleppoMessages.getString("RecordConnector.20") + //$NON-NLS-1$
                                    processors.elementAt(i).getClass().toString());
                            break;
                        }
                    }
                }

                if (recin != null) {
                    if (!headerSet && recin != null && doHeader) {
                        for (int i = 0; i < dst.size(); i++) {
                            dst.elementAt(i).setHeaderRecord(recin);
                        }
                        headerSet = true;
                    }

                    switch (mode) {
                        case CONNECT_MODE_RANDOM:
                            split_index = rangen.nextInt(dst.size());
                            retval = dst.elementAt(split_index).setRecord(recin) && retval;
                            break;
                        case CONNECT_MODE_SPLIT:
                            if (split_index > dst.size() - 1) {
                                split_index = 0;
                            }
                            retval = dst.elementAt(split_index++).setRecord(recin) && retval;
                            break;
                        default:
                            for (int i = 0; i < dst.size(); i++) {
                                retval = dst.elementAt(i).setRecord(recin) && retval;
                            }
                    }

                    if (retval) {
                        delivered++;
                    }


                    logger.debug(AleppoMessages.getString("RecordConnector.11") + recin); //$NON-NLS-1$
                }
            }
            // GFA: Changed the concept of moved to mean move attempts, rather than data moving through the
            //      system.
            moved++;

        }
        return retval;
    }

    public long moveAll() {
        // Start with a negative return value in case the system fails to move anything.
        long retval = 0;
        logger.debug(AleppoMessages.getString("RecordConnector.12") + doHeader + AleppoMessages.getString("RecordConnector.13") + maxrows); //$NON-NLS-1$ //$NON-NLS-2$
        if (dst != null || src != null) {

//			if (doHeader && dst != null && src != null) {
//				RecordCapsule temp = src.getHeaderRecord();
//				for (int i = 0; i < dst.size(); i++) {
//					if (processors != null && processors.size() > 0) {
//						for (int j = 0; j < processors.size(); i++) {
//							logger.debug(AleppoMessages.getString("RecordConnector.14") + processors.elementAt(j)); //$NON-NLS-1$
//							temp = processors.elementAt(j).processRecord(temp);
//							if (temp == null) {
//								break;
//							}
//						}
//					}
//					if( !headerSet && temp != null ){
//						dst.elementAt(i).setHeaderRecord(temp);	
//						headerSet = true;
//					}
//				}
//			}

            while (moved < maxrows && moveRecord()) {
                logger.debug(AleppoMessages.getString("RecordConnector.15") + moved); //$NON-NLS-1$
                Thread.yield();
            }
            retval = moved;

        }
        if (retval < minrows) {
            retval = -1L;
        }

        close();
        return retval;
    }

    public boolean setDoHeader(boolean action) {
        doHeader = action;
        return true;
    }

    public boolean setFilter(RecordFilter f) {
        processors.add(f);
        return true;
    }

    public boolean addObserver(Object f) {
        if (f != null) {
            observers.add(f);
        }
        return true;
    }

    public boolean addProcess(RecordProcessor f) {
        logger.debug(AleppoMessages.getString("RecordConnector.16") + f); //$NON-NLS-1$
        processors.add(f);
        return true;
    }

    public boolean setMinRows(long l) {
        boolean retval = false;
        if (l >= 0) {
            minrows = l;
            logger.debug(AleppoMessages.getString("RecordConnector.24") + l); //$NON-NLS-1$
            retval = true;
        }
        return retval;
    }

    public boolean setMaxRows(long l) {
        boolean retval = false;
        if (l > 0) {
            maxrows = l;
            logger.debug(AleppoMessages.getString("RecordConnector.17") + l); //$NON-NLS-1$
            retval = true;
        }
        return retval;
    }

    public boolean setMode(int m) {
        boolean retval = false;
        if (m >= CONNECT_MODE_MIN && m <= CONNECT_MODE_MAX) {
            mode = m;
            logger.debug(this.toString() + AleppoMessages.getString("RecordConnector.3") + mode); //$NON-NLS-1$
            retval = true;
        }
        return retval;
    }

    @Override
    public boolean doInitialization() {

        //
        if (pmap.containsKey(AleppoConstants.ALEPPO_CONFIG_CONNECTOR_PARAM_DOHEADER)) {
            setDoHeader(Boolean.parseBoolean((String) pmap.get(AleppoConstants.ALEPPO_CONFIG_CONNECTOR_PARAM_DOHEADER)));
        }

        // Load the properties file if an attribute is found.
        if (pmap.containsKey(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_FILE)) {
            props = MiscUtil.loadXMLPropertiesFile((String) pmap.get(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_FILE));

            if (props == null) {
                logger.error(AleppoMessages.getString("RecordConnector.21")); //$NON-NLS-1$
            } else {
                logger.debug(AleppoMessages.getString("RecordConnector.22")); //$NON-NLS-1$
                // Add all the current environment variables to the properties.
                Map<String, String> sysenv = System.getenv();
                Iterator<String> siterator = sysenv.keySet().iterator();
                while (siterator.hasNext()) {
                    String key = siterator.next();
                    String value = sysenv.get(key);
                    props.setProperty(key, value);
                }

                // Try to replace tokens that are embedded in property values with the values
                // for those tokens, if the token is found to be a property key.
                Enumeration<Object> foo = props.keys();
                while (foo.hasMoreElements()) {
                    String bar = (String) foo.nextElement();
                    String value = props.getProperty(bar);
                    if (value.contains("%")) {
                        value = MiscUtil.mapString(props, value, "%");
                        props.setProperty(bar, value);
                    }
                }
            }
        }
        try {
            if (pmap.containsKey(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_MAXROWS)) {
                this.setMaxRows(Long.parseLong((String) pmap.get(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_MAXROWS)));
            }
        } catch (NumberFormatException e) {
            logger.error(AleppoMessages.getString("RecordConnector.18") //$NON-NLS-1$
                    + AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_MAXROWS);
        }
        try {
            if (pmap.containsKey(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_MINROWS)) {
                this.setMinRows(Long.parseLong((String) pmap.get(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_MINROWS)));
            }
        } catch (NumberFormatException e) {
            logger.error(AleppoMessages.getString("RecordConnector.18") //$NON-NLS-1$
                    + AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_MAXROWS);
        }

        return false;
    }

    private boolean addGlobalProperties(RecordCapsule recin) {
        // If there is already a global set of properties, then add/update the properties with
        // the values read from this process node.

        if (recin.getMetaData(AleppoConstants.ALEPPO_DC_MDATA_PROPERTIES_KEY) != null) {
            Properties temp = (Properties) recin.getMetaData(AleppoConstants.ALEPPO_DC_MDATA_PROPERTIES_KEY);
            Iterator<Object> it = props.keySet().iterator();
            while (it.hasNext()) {
                String key = (String) it.next();
                temp.put(key, props.get(key));
            }
        } else {
            recin.setMetaData(AleppoConstants.ALEPPO_DC_MDATA_PROPERTIES_KEY, props.clone());
            logger.debug(AleppoMessages.getString("RecordConnector.23"));                 //$NON-NLS-1$
        }
        return true;
    }

    public void run() {

        logger.debug(this.toString() + AleppoMessages.getString("RecordConnector.4")); //$NON-NLS-1$
        moveAll();
        //close();
        setChanged();
        notifyObservers(new Long(delivered));
    }

    private long maxrows = Long.MAX_VALUE;
    private long minrows = 0L;
    //protected Properties props = null;
    private Random rangen = new Random();
    private AbstractRecordSource src = null;
    private Vector<RecordDestination> dst = new Vector<RecordDestination>();
    // private RecordFilter filt = null;
    private long moved = 0L;
    private long delivered = 0L;
    private boolean doHeader = false;
    Vector<RecordProcessor> processors = new Vector<RecordProcessor>();
    Vector<Object> observers = new Vector<Object>();
    private int split_index = 0;

    private int mode = CONNECT_MODE_DUPLICATE;
    private boolean headerSet = false;

    public static final int CONNECT_MODE_MIN = 0;
    public static final int CONNECT_MODE_DUPLICATE = 0;
    public static final int CONNECT_MODE_SPLIT = 1;
    public static final int CONNECT_MODE_RANDOM = 2;
    public static final int CONNECT_MODE_MAX = 2;

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("RecordConnector");


}
