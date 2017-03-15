/**
 *
 */
package com.codeondemand.javapeppers.habanero.util.xml;

import com.codeondemand.javapeppers.habanero.util.misc.UtlRunnable;

import javax.xml.transform.Transformer;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * The XMLListProcessor is a class that needs to be extended to provide
 * anything more than an echo of the list items to standard output. It is
 * expected to take an incoming String and process it with an XSL
 * transform.  This class should really not be used, but this type
 * of processing should be done using the Aleppo dataflow framework which
 * contains functionality for processing streams of data using an XML
 *
 * @deprecated Use the com.codeondemand.javapeppers.aleppo framework
 */
public class XMLListProcessor extends UtlRunnable {

    //***********************************************************************
    // Constructors
    //***********************************************************************
    public XMLListProcessor() {
    }

    //***********************************************************************
    // Public methods and data
    //***********************************************************************

    public void initialize(String stylefile) {
        t = XMLTransformerFactory.createTransformer(stylefile);
    }

    public ConcurrentLinkedQueue<String> getEventQueue() {
        return itemQ;
    }

    public void run() {
        try {
            while (true) {
                // If we start through the loop with an empty queue
                // then we need to get if filled by a call to the
                // observer responsible for providing information
                if (itemQ.isEmpty()) {
                    setChanged();
                    notifyObservers();
                }
                // If the queue is still empty when we come back from
                // the provider, then we are done.
                if (itemQ.isEmpty()) {
                    finished();
                    break;
                }

                // Process all of the events.
                while (!itemQ.isEmpty()) {
                    String item = (String) itemQ.poll();
                    String result = processItem(t, item);
                    writeItem(result);
                }
                Thread.yield();
            }
        } catch (Exception e0) {
            e0.printStackTrace();
        }
    }

    protected static String processItem(Transformer t, String item) {
        return item;
    }

    protected static void writeItem(String item) {
        System.out.println(item);

    }

    protected void finished() {
        //TO-DO: Need to do something?
    }

    //***********************************************************************
    // Private data and methods
    //***********************************************************************
    private ConcurrentLinkedQueue<String> itemQ = new ConcurrentLinkedQueue<String>();
    private Transformer t = null;
}
