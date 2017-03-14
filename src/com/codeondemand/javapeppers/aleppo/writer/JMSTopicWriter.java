/**
 * 
 */
package com.codeondemand.javapeppers.aleppo.writer;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * @author Gary Anderson
 * 
 */
public class JMSTopicWriter extends DestinationWriter {

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.javapeppers.aleppo.writer.DestinationWriter#close()
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
	 * @see com.javapeppers.aleppo.writer.DestinationWriter#reset()
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
	 * @see com.javapeppers.aleppo.writer.DestinationWriter#write(java.lang.Object)
	 */
	@Override
	public boolean write(Object data) {
		boolean retval = true;

		if (!initialized) {
			initialized = doInitialization();
		}
		if (initialized) {
			TextMessage msg;
			try {
				msg = session.createTextMessage(data.toString());
				prod.send(msg);
			} catch (JMSException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			retval = false;
		}
		return retval;
	}

	private boolean doInitialization() {
		boolean retval = false;
		String jms_url = ActiveMQConnection.DEFAULT_BROKER_URL;
		String jms_usr = ActiveMQConnection.DEFAULT_USER;
		String jms_pwd = ActiveMQConnection.DEFAULT_PASSWORD;
		String jms_topic = "topic";

		if (pmap.containsKey("jms.host") && pmap.containsKey("jms.port")) {
			jms_url = "tcp://" + pmap.get("jms.host") + ":"
					+ pmap.get("jms.port");
		}
		ActiveMQConnectionFactory f = new ActiveMQConnectionFactory(jms_usr,
				jms_pwd, jms_url);

		try {
			c = f.createConnection();
			c.start();
			session = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
			if (pmap.containsKey("jms.topic")) {
				jms_topic = (String) pmap.get("jms.topic");
			}
			topic = session.createTopic(jms_topic);
			prod = session.createProducer(topic);
			retval = true;
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return retval;
	}

	private Destination topic = null;
	private Connection c = null;
	private Session session = null;
	private MessageProducer prod = null;

	private boolean initialized = false;

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}
}
