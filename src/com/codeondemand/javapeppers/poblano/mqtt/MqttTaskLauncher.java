package com.codeondemand.javapeppers.poblano.mqtt;

import com.codeondemand.javapeppers.poblano.mqtt.base.MqttBrokerInfo;
import com.codeondemand.javapeppers.poblano.mqtt.task.MqttTaskInfo;
import com.codeondemand.javapeppers.poblano.mqtt.task.MqttTaskNode;
import net.sf.saxon.s9api.*;
import org.apache.logging.log4j.LogManager;

import java.io.File;
import java.util.Hashtable;

public class MqttTaskLauncher {

    /**
     * Startup all the demo components after parsing the common control file.
     *
     * @param configFile -  Specifies the name of the configuration resource file.
     */
    public void initialize(String configFile) {
        try {
            Processor p = new Processor(false);
            DocumentBuilder builder = p.newDocumentBuilder();
            File cfile = new File(configFile);
            XdmNode doc = builder.build(cfile);

            String expression = "/config/brokers/broker";
            XPathCompiler xpc = p.newXPathCompiler();
            XPathExecutable xpe = xpc.compile(expression);
            XPathSelector xps = xpe.load();
            xps.setContextItem(doc);
            XdmValue brokers = xps.evaluate();

            if (brokers != null && brokers.size() > 0) {
                buildBrokerTable(brokers);
            }

            expression = "/config/tasks/task";
            xpe = xpc.compile(expression);
            xps = xpe.load();
            xps.setContextItem(doc);
            XdmValue tasks = xps.evaluate();
            if (tasks != null && tasks.size() > 0) {
                buildTaskTable(tasks);
            }

        } catch (SaxonApiException sape) {
            logger.error(sape.toString());
        }

    }

    /**
     * This takes a portion of the configuration document in the form of a
     * XDMNode that represents the tasks that need to be launched.  The
     * attributes of the task are parsed and the task is launched.
     *
     * @param tasks
     */
    private void buildTaskTable(XdmValue tasks) {
        int foo = tasks.size();
        for (int i = 0; i < foo; i++) {
            MqttTaskInfo info = null;

            XdmNode task = (XdmNode) tasks.itemAt(i);
            String tname = null;
            String pub_broker = null;
            String sub_broker = null;
            String pub_topic = null;
            String sub_topic = null;
            String qos = null;
            String classname = null;

            QName n = new QName("name");
            XdmSequenceIterator it = task.axisIterator(net.sf.saxon.s9api.Axis.ATTRIBUTE, n);
            while (it.hasNext()) {
                XdmItem item = it.next();
                tname = item.getStringValue();
            }

            // Only proceed if there is a name
            if (tname != null) {
                info = new MqttTaskInfo(tname);
            } else {
                continue;
            }

            n = new QName("classname");
            it = task.axisIterator(net.sf.saxon.s9api.Axis.ATTRIBUTE, n);
            while (it.hasNext()) {
                XdmItem item = it.next();
                classname = item.getStringValue();
                info.setClassname(classname);
            }
            n = new QName("qos");
            it = task.axisIterator(net.sf.saxon.s9api.Axis.ATTRIBUTE, n);
            while (it.hasNext()) {
                XdmItem item = it.next();
                qos = item.getStringValue();
                info.setQos(qos);
            }

            n = new QName("pub_broker");
            it = task.axisIterator(net.sf.saxon.s9api.Axis.ATTRIBUTE, n);
            while (it.hasNext()) {
                XdmItem item = it.next();
                pub_broker = item.getStringValue();
                info.setPub_broker(pub_broker);
            }

            n = new QName("sub_broker");
            it = task.axisIterator(net.sf.saxon.s9api.Axis.ATTRIBUTE, n);
            while (it.hasNext()) {
                XdmItem item = it.next();
                sub_broker = item.getStringValue();
                info.setSub_broker(sub_broker);
            }

            n = new QName("pub_topic");
            it = task.axisIterator(net.sf.saxon.s9api.Axis.ATTRIBUTE, n);
            while (it.hasNext()) {
                XdmItem item = it.next();
                pub_topic = item.getStringValue();
                info.setPub_topic(pub_topic);
            }

            n = new QName("sub_topic");
            it = task.axisIterator(net.sf.saxon.s9api.Axis.ATTRIBUTE, n);
            while (it.hasNext()) {
                XdmItem item = it.next();
                sub_topic = item.getStringValue();
                info.setSub_topic(sub_topic);
            }

            n = new QName("arg");
            it = task.axisIterator(net.sf.saxon.s9api.Axis.DESCENDANT, n);
            while (it.hasNext()) {
                XdmNode item = (XdmNode) it.next();
                QName n1 = new QName("field");
                QName n2 = new QName("value");
                String key = null;
                String value = null;
                XdmSequenceIterator it2 = item.axisIterator(net.sf.saxon.s9api.Axis.ATTRIBUTE, n1);
                if (it2.hasNext()) {
                    key = it2.next().getStringValue();
                }
                it2 = item.axisIterator(net.sf.saxon.s9api.Axis.ATTRIBUTE, n2);
                if (it2.hasNext()) {
                    value = it2.next().getStringValue();
                }
                if (key != null && value != null) {
                    info.setField(key, value);
                }
            }
            startTask(info);
        }

    }

    private void buildBrokerTable(XdmValue brokers) {
        int foo = brokers.size();
        for (int i = 0; i < foo; i++) {
            XdmNode broker = (XdmNode) brokers.itemAt(i);
            String bname = null;
            String bport = null;
            String bip = null;
            QName n = new QName("name");
            XdmSequenceIterator it = broker.axisIterator(net.sf.saxon.s9api.Axis.ATTRIBUTE, n);
            while (it.hasNext()) {
                XdmItem item = it.next();
                bname = item.getStringValue();
            }

            n = new QName("ip");
            it = broker.axisIterator(net.sf.saxon.s9api.Axis.ATTRIBUTE, n);
            while (it.hasNext()) {
                XdmItem item = it.next();
                bip = item.getStringValue();
            }

            n = new QName("port");
            it = broker.axisIterator(net.sf.saxon.s9api.Axis.ATTRIBUTE, n);
            while (it.hasNext()) {
                XdmItem item = it.next();
                bport = item.getStringValue();
            }
            System.out.println(bname + ":" + bip + ":" + bport);
            MqttBrokerInfo binfo = new MqttBrokerInfo(bname, bip, bport);
            btable.put(bname, binfo);
        }

    }

    /**
     * This method attempts to instantiate a class. Note that the class a
     * extension of the MqttTask class or it will not be instantiated.
     * <p>
     * If the class is an instance of MqttTask then it is instantiated and
     * initialized with the information read from the configuration file
     * (such as the broker to use, topics, qos, etc)
     *
     * @param info A MqttTaskInfo object with the information for
     *             launching the task.
     */
    public void startTask(MqttTaskInfo info) {
        try {
            Object foo = Class.forName(info.getClassname()).newInstance();
            if (MqttTaskNode.class.isInstance(foo)) {

                logger.debug("instantiating:" + foo + "\n");
                if (foo != null) {
                    MqttTaskNode bar = (MqttTaskNode) foo;
                    bar.setTaskName(info.getTaskname());
                    bar.setPubBroker(btable.get(info.getPub_broker()));
                    bar.setSubBroker(btable.get(info.getSub_broker()));
                    bar.setPub_topic(info.getPub_topic());
                    bar.setSub_topic(info.getSub_topic());
                    if (info.getQos() != null) {
                        byte[] temp = info.getQos().getBytes();
                        bar.setQos(temp[0]);
                    }
                    if (info.getFields() != null) {
                        bar.setFields(info.getFields());
                    }
                    bar.initialize();

                }
            } else {
                logger.error(info.getClassname() + " is not an instance of the MqttTask class\n");
            }
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    // ***********************************************************************
    // Private data and methods
    // ***********************************************************************

    private Hashtable<String, MqttBrokerInfo> btable = new Hashtable<String, MqttBrokerInfo>();
    //private Hashtable<String, MqttTaskInfo> ttable = new Hashtable<String, MqttTaskInfo>();

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("MqttTaskLauncher");

}