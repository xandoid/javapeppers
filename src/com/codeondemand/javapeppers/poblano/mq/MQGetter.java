// --------------------------------------------------------------------------
//  javapeppers Confidential
//  
//
// This code is a modification of sample code received from Brent Cooley
// bcooley@us.javapeppers.com
// --------------------------------------------------------------------------
package com.codeondemand.javapeppers.poblano.mq;

import com.ibm.mq.*;

public class MQGetter {

    //***********************************************************************
    // Constructors
    //***********************************************************************
    public MQGetter() {
        //  MQEnvironment.hostname  = hostname;
        //  MQEnvironment.port      = port;
        //  MQEnvironment.channel   = channel;
    }

    //***********************************************************************
    // Public methods and data
    //***********************************************************************
    /*
    *  Connects to a Queue manager and queue for input and inquiry
    *
    *  @param qm    The Queue Manager to connect.
    *  @param queue The named queue to connect
    *
    *  @return true if successful otherwise false
    */
    public boolean connect(String qm, String queue) {
        boolean success = false;
        try {
            int openOptions = MQC.MQOO_INPUT_AS_Q_DEF | MQC.MQOO_INQUIRE;
            mq_q_mgr = new MQQueueManager(qm);
            requestQueue = mq_q_mgr.accessQueue(queue, openOptions, null, null, null);
            success = true;
        } catch (Exception ex) {
            System.out.println(ex);
            ex.printStackTrace();
        }
        return success;
    }

    /**
     * Disconnects from the Queue manager.
     */
    public void disconnect() {
        try {
            mq_q_mgr.disconnect();
            requestQueue = null;
        } catch (Exception ex) {
            System.out.println(ex);
            ex.printStackTrace();
        }
    }

    /**
     * Gets a message from the connected MQ queue.
     *
     * @return The next message on the MQ queue.
     */
    public MQMessage getMessage() {
        MQMessage message = null;
        try {
            if ((requestQueue != null) && requestQueue.getCurrentDepth() > 0) {
                message = new MQMessage();
                MQGetMessageOptions gmo = new MQGetMessageOptions();
                requestQueue.get(message, gmo);
            }
        } catch (Exception ex) {
            System.out.println(ex);
            ex.printStackTrace();
        }
        return message;
    }

    /**
     * Gets a text type payload from an MQ message
     *
     * @param message An MQmessage object read from a queue.
     * @return A String with the text payload of the message.
     */
    public static String getTextPayload(MQMessage message) {
        String payload = null;
        try {
            if (message != null) {
                // Move to the place to read the header length
                message.seek(8);
                int h3 = message.readInt4();
                message.seek(h3);
                payload = message.readStringOfByteLength(message.getMessageLength() - h3);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return payload;
    }


    public static void main(String[] args) {
        if (args.length < 5) {
            System.out.println("USAGE: QM hostname port channel queue count");
            System.exit(1);
        }
        String qm = args[0];
        String hostname = args[1];
        int port = Integer.parseInt(args[2]);
        String channel = args[3];
        String queue = args[4];
        int msg_count = 1;
        if (args.length == 6) {
            msg_count = Integer.parseInt(args[5]);
        }
        MQEnvironment.channel = channel;
        MQEnvironment.hostname = hostname;
        MQEnvironment.port = port;
        MQGetter foo = new MQGetter();

        try {
            foo.connect(qm, queue);

            while (msg_count-- > 0) {
                MQMessage message = foo.getMessage();
                if (message != null) {

                    System.out.println("Message format: " + message.format);
                    System.out.println("Message length: " + message.getMessageLength());

                    message.seek(8);
                    int h3 = message.readInt4();
                    System.out.println("Header length: " + h3);


                    System.out.println("Message text: ");
                    System.out.println(MQGetter.getTextPayload(message));

                } else {
                    System.out.println("No messages available");
                    msg_count = 0;
                }

            }

            foo.disconnect();
        }

        // If an error has occured in the above, try to identify what went wrong.
        // Was it an MQ error?
        catch (Exception ex) {
            System.out.println(ex);
            ex.printStackTrace();
        }
    }

    //***********************************************************************
    // Private data and methods
    //***********************************************************************
    private MQQueueManager mq_q_mgr = null;
    private MQQueue requestQueue = null;
}


