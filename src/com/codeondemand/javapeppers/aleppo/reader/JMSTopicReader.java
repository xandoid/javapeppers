/**
 *
 */
package com.codeondemand.javapeppers.aleppo.reader;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * @author Gary Anderson
 */
public class JMSTopicReader extends SourceReader implements MessageListener {

    /*
     * (non-Javadoc)
     *
     * @see com.javapeppers.aleppo.reader.SourceReader#close()
     */
    @Override
    public boolean close() {
        try {
            if (session != null) {
                session.close();
            }
            if (c != null) {
                c.stop();
                c.close();
            }
        } catch (JMSException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return true;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.javapeppers.aleppo.reader.SourceReader#reset()
     */
    @Override
    public boolean reset() {
        close();
        initialized = doInitialization();
        return initialized;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.javapeppers.aleppo.reader.SourceReader#read()
     */
    @Override
    public Object read() {
        while (!received) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        synchronized (this) {
            received = false;
            return new String(message);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see com.javapeppers.aleppo.common.FlowNode#doInitialization()
     */
    @Override
    public boolean doInitialization() {
        boolean retval = false;
        String jms_url = ActiveMQConnection.DEFAULT_BROKER_URL;
        String jms_usr = ActiveMQConnection.DEFAULT_USER;
        String jms_pwd = ActiveMQConnection.DEFAULT_PASSWORD;
        String jms_topic = "topic";
        if (pmap.containsKey("jms.host") && pmap.containsKey("jms.port")) {
            jms_url = "tcp://" + pmap.get("jms.host") + ":" + pmap.get("jms.port");
        }
        ActiveMQConnectionFactory f = new ActiveMQConnectionFactory(jms_usr, jms_pwd, jms_url);

        try {
            c = f.createConnection();
            c.start();
            session = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            if (pmap.containsKey("jms.topic")) {
                jms_topic = (String) pmap.get("jms.topic");
            }
            dest = session.createTopic(jms_topic);
            consumer = session.createConsumer(dest);
            consumer.setMessageListener(this);
            retval = true;
        } catch (JMSException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return retval;
    }

    private Destination dest = null;
    private Connection c = null;
    private Session session = null;
    private MessageConsumer consumer = null;

    private boolean initialized = false;
    private boolean received = false;
    private String message = null;

    /* (non-Javadoc)
     * @see javax.jms.MessageListener#onMessage(javax.jms.Message)
     */
    public synchronized void onMessage(Message arg0) {
        if (arg0 instanceof TextMessage) {
            TextMessage msg = (TextMessage) arg0;
            try {
                message = msg.getText();
                received = true;
            } catch (JMSException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } else {
            System.out.println(arg0.toString());
        }
    }
}
