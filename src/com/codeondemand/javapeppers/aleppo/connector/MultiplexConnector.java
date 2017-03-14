package com.codeondemand.javapeppers.aleppo.connector;

import java.util.Observable;
import java.util.Observer;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;

import com.codeondemand.javapeppers.aleppo.AleppoMessages;
import com.codeondemand.javapeppers.aleppo.common.RecordProcessor;
import com.codeondemand.javapeppers.aleppo.destination.RecordDestination;
import com.codeondemand.javapeppers.aleppo.source.RecordSource;

public class MultiplexConnector extends Observable implements Observer {

	public boolean initialize(RecordSource[] srcs, RecordDestination[] dsts,
			RecordProcessor[] filts) {

		boolean retval = true;

		if (srcs.length == dsts.length) {
			for (int i = 0; i < srcs.length; i++) {
				RecordConnector s2d = createNewConnector();
				retval = retval && s2d.initialize(srcs[i], dsts[i], filts);
				Thread t = new Thread(s2d);
				t.start();
			}
		}

		initialized();
		return retval;

	}

	public boolean initialize(RecordSource[] srcs, RecordDestination dst,
			RecordProcessor[] filt) {

		boolean retval = true;

		for (int i = 0; i < srcs.length; i++) {
			RecordConnector s2d = createNewConnector();
			retval = retval && s2d.initialize(srcs[i], dst, filt);
			Thread t = new Thread(s2d);
			t.start();
		}
		initialized();
		return retval;

	}

	public boolean initialize(RecordSource src, RecordDestination[] dsts,
			RecordProcessor[] filt) {

		boolean retval = true;
		RecordConnector s2d = createNewConnector();
		retval = retval && s2d.initialize(src, dsts[0], filt);
		for (int i = 1; i < dsts.length; i++) {
			retval = retval && s2d.addDestination( dsts[i]);
		}
		Thread t = new Thread(s2d);
		t.start();
		initialized();
		return retval;
	}

	private RecordConnector createNewConnector() {
		RecordConnector retval = new RecordConnector();
		connectors.add(retval);
		retval.setMode(mode);
		retval.setDoHeader(doHeader);
		retval.addObserver(this);
		incrementActiveSrcCount();
		return retval;
	}

	public boolean setDoHeader(boolean value) {
		boolean retval = true;
		doHeader = value;
		for (int i = 0; i < connectors.size(); i++) {
			retval = retval && connectors.elementAt(i).setDoHeader(value);
		}
		return retval;
	}

	public synchronized void initialized() {
		initialized = true;
	}

	public void incrementActiveSrcCount() {
		activesrccount++;
	}

	public void decrementActiveSrcCount() {
		activesrccount--;
	}
	public boolean setMode(int m){
		boolean retval = false;
		if( m >= RecordConnector.CONNECT_MODE_MIN && m <= RecordConnector.CONNECT_MODE_MAX){
			mode = m;
			logger.debug(this.toString()+AleppoMessages.getString("MultiplexConnector.0") + mode); //$NON-NLS-1$
			retval = true;
		}
		return retval;
	}

	public synchronized void update(Observable o, Object arg) {

		logger.debug(AleppoMessages.getString("MultiplexConnector.1") + o.toString() + AleppoMessages.getString("MultiplexConnector.2") //$NON-NLS-1$ //$NON-NLS-2$
				+ arg.toString());
		if (o instanceof RecordConnector) {
			decrementActiveSrcCount();
			if (activesrccount < 1 && initialized) {
				setChanged();
				notifyObservers();
			}
		}
	}

	private boolean initialized = false;
	private int mode = RecordConnector.CONNECT_MODE_DUPLICATE;
	private int activesrccount = 0;
	private boolean doHeader = false;
	private Vector<RecordConnector> connectors = new Vector<RecordConnector>();
	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("MultiplexConnector");

}
