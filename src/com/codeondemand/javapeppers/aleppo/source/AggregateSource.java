package com.codeondemand.javapeppers.aleppo.source;

import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import org.apache.logging.log4j.LogManager;

import java.util.Vector;

/**
 * The AggregateSource class provides the functionality to concatenate a number
 * sources into a single source.  This implementation of the class does not
 * consider the record keys, so it just is a simple concatenator for as many
 * sources as it finds.  When one source is exhausted, then the class will
 * return a null for the next record request.
 *
 * @author gfa
 */
public class AggregateSource extends RecordSource {

    /**
     * Closes all of the underlying record sources.
     */
    @Override
    public boolean closeSource() {
        boolean retval = false;
        if (sources != null) {
            for (RecordSource source : sources) {
                source.closeSource();
            }
            retval = true;
        }
        return retval;
    }

    @Override
    public RecordCapsule getCurrentRecord() {
        RecordCapsule retval = null;
        if (sources != null) {
            for (RecordSource source : sources) {
                retval = combine(retval, source.getCurrentRecord());
            }
        }
        return retval;
    }

    @Override
    public RecordCapsule getHeaderRecord() {
        RecordCapsule retval = null;
        if (sources != null) {
            for (RecordSource source : sources) {
                retval = combine(retval, source.getHeaderRecord());
            }
        }
        return retval;
    }

    @Override
    public RecordCapsule getNextRecord() {
        RecordCapsule retval = null;
        boolean finished = false;
        if (sources != null) {

            for (RecordSource source1 : sources) {
                if (source1.getNextRecord() == null) {
                    finished = true;
                    retval = null;
                    break;
                }
            }
            if (!finished) {
                for (RecordSource source : sources) {
                    logger.debug(source.getCurrentRecord());
                    retval = combine(retval, source.getCurrentRecord());
                }
            }
        }
        return retval;
    }


    public boolean initialize(Object[] args) {
        boolean retval = false;
        if (args != null && args.length > 0 && args[0] instanceof RecordSource) {
            sources = new Vector<>(args.length);
            for (Object arg : args) {
                if (arg instanceof RecordSource) {
                    sources.add((RecordSource) arg);
                    logger.debug("Adding record source" + arg.toString());
                } else {
                    sources.clear();
                    retval = false;
                    break;
                }
                retval = true;
            }
        }

        return retval;
    }

    @Override
    public boolean reset() {
        boolean retval = false;
        if (sources != null) {
            for (RecordSource source : sources) {
                source.reset();
            }
            retval = true;
        }
        return retval;
    }

    protected RecordCapsule combine(RecordCapsule current, RecordCapsule append) {
        RecordCapsule retval = current;

        if (retval == null) {
            retval = append;
        } else {
            for (int i = 0; i < append.getFieldCount(); i++) {
                current.addDataCapsule(append.getField(i), false);
            }
        }
        return retval;
    }

    public boolean setSource(RecordSource src) {
        boolean retval = false;
        if (sources == null) {
            sources = new Vector<>();
            retval = sources.add(src);
        } else {
            sources.add(src);
        }

        return retval;
    }

    protected Vector<RecordSource> sources = null;
    protected Object delim = new String();


    public Object[] getCurrentRecordNoKey() {
        return null;
    }

    public Object getHeaderRecordNoKeys() {
        return null;
    }

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("AggregateSource");
}
