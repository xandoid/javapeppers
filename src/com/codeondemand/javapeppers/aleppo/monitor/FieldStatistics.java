/**
 *
 */
package com.codeondemand.javapeppers.aleppo.monitor;

import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.aleppo.writer.FileRecordWriter;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.logging.log4j.LogManager;

import java.util.Iterator;
import java.util.TreeMap;

public class FieldStatistics extends MonitorProcess {

    public void done() {
        Iterator<String> i = stats.keySet().iterator();
        StringBuffer sb = new StringBuffer();
        while (i.hasNext()) {
            String key = i.next().toString();
            sb.append("\nStatistics for " + key + "\n");
            if (stats.get(key) instanceof SummaryStatistics) {
                sb.append(stats.get(key).toString());
            } else {
                DescriptiveStatistics temp = (DescriptiveStatistics) stats.get(key);
                sb.append(temp.toString());
                // Now percentiles
                sb.append("10th percentile: " + temp.getPercentile(10.0));
                sb.append("\n25th percentile: " + temp.getPercentile(25.0));
                sb.append("\n75th percentile: " + temp.getPercentile(75.0));
                sb.append("\n90th percentile: " + temp.getPercentile(90.0));
                sb.append("\n\n");
            }
        }
        if (pmap.containsKey("file")) {
            FileRecordWriter fwrtr = new FileRecordWriter();
            if (fwrtr.initialize(pmap.get("file").toString(), false)) {
                fwrtr.write(sb.toString());
                fwrtr.close();
            } else {
                logger.debug(sb.toString());
            }

        }
    }

    public boolean reset() {
        Iterator<String> i = stats.keySet().iterator();
        while (i.hasNext()) {
            String key = i.next().toString();
            if (stats.get(key) instanceof SummaryStatistics) {
                ((SummaryStatistics) stats.get(key)).clear();
            } else {
                ((org.apache.commons.math3.stat.descriptive.DescriptiveStatistics) stats.get(key)).clear();
            }
        }
        return true;
    }

    @Override
    protected boolean doMonitor(RecordCapsule rc) {
        for (int i = 0; i < rc.getFieldCount(); i++) {
            DataCapsule dc = rc.getField(i);
            if (stats.containsKey(dc.getName()) && !dc.isNull()) {
                if (stats.get(dc.getName()) instanceof SummaryStatistics) {
                    ((SummaryStatistics) stats.get(dc.getName())).addValue(new Double(dc.getData().toString()).doubleValue());
                } else {
                    ((DescriptiveStatistics) stats.get(dc.getName())).addValue(new Double(dc.getData().toString()).doubleValue());
                }
            }
        }
        return true;
    }

    @Override
    public boolean doInitialization() {
        if (processData != null) {
            initialize(processData);
        }
        return true;
    }

    @Override
    public boolean initialize(RecordCapsule rc) {
        for (int i = 0; i < rc.getFieldCount(); i++) {
            DataCapsule dc = rc.getField(i);

            if (dc.getMetaData("stat_type") != null && dc.getMetaData("stat_type").toString().equalsIgnoreCase("summary")) {
                stats.put(dc.getName(), new SummaryStatistics());
            } else {
                stats.put(dc.getName(), new DescriptiveStatistics());
            }
        }
        return true;
    }

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("FieldStatistics");

    protected TreeMap<String, Object> stats = new TreeMap<String, Object>();
}
