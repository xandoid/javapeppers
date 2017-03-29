package com.codeondemand.javapeppers.aleppo.monitor;

import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;

import javax.json.Json;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObjectBuilder;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.GregorianCalendar;


public class FlowCountMonitor extends MonitorProcess {

    @Override
    public boolean initialize(RecordCapsule rc) {
        return true;
    }

    public void done() {
        long current_time = new GregorianCalendar().getTimeInMillis();
        double elapsed_time_sec = (current_time - start_time) / 1000.0;
        if (decimals) {
            System.out.println(name + ":" + (getRecordCount() + 1) / elapsed_time_sec + ":" + (getRecordCount() + 1));

        } else {
            System.out.println(new Double((getRecordCount() + 1) / elapsed_time_sec).longValue() + ":" + (getRecordCount() + 1));

        }
    }

    @Override
    protected boolean doMonitor(RecordCapsule input) {
        if (!initialized) {
            if (pmap.containsKey("interval")) {
                interval = Long.parseLong((String) pmap.get("interval"));
            }
            if (pmap.containsKey("decimals")) {
                decimals = Boolean.parseBoolean((String) pmap.get("decimals"));
            }
            if (pmap.containsKey("name")) {
                name = (String) pmap.get("name");
            }
            if (pmap.containsKey("show_time")) {
                show_time = Boolean.parseBoolean((String) pmap.get("show_time"));
            }
            if (pmap.containsKey("do_notify")) {
                do_notify = Boolean.parseBoolean((String) pmap.get("do_notify"));
            }

            start_time = new GregorianCalendar().getTimeInMillis();
            interval_time = start_time;
            initialized = true;
        }
        counter++;

        if (counter == interval) {
            long current_time = new GregorianCalendar().getTimeInMillis();
            double interval_time_sec = (current_time - interval_time) / 1000.0;
            double tot_time_sec = (current_time - start_time) / 1000.0;
            Long cnt = getRecordCount() + 1;
            Double intervalPerSec = (double) interval / interval_time_sec;
            Double totPerSec = new Double(cnt) / tot_time_sec;
            if (decimals) {
                showCount(name, cnt, String.valueOf(totPerSec), String.valueOf(intervalPerSec));
            } else {
                showCount(name, cnt, totPerSec.longValue(), intervalPerSec.longValue());
            }
            counter = 0L;
            interval_time = new GregorianCalendar().getTimeInMillis();

            if (do_notify) {
                String hostname = null;
                try {
                    hostname = InetAddress.getLocalHost().getHostName();
                } catch (UnknownHostException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

                JsonBuilderFactory factory = Json.createBuilderFactory(null);
                JsonObjectBuilder bldr = factory.createObjectBuilder();
                bldr.add("timestamp", MiscUtil.getCurrentTimeString());
                bldr.add("name", this.getClass().getSimpleName());
                bldr.add("hostname", hostname);
                bldr.add("tot_per_sec", totPerSec.longValue());
                bldr.add("interval_per_sec", intervalPerSec.longValue());
                bldr.add("count", cnt);
                String foo = bldr.build().toString();
                this.setChanged();
                this.notifyObservers(foo);
            }

        }
        return true;
    }

    protected void showCount(String name, Long cnt, Long totPerSec, Long intervalPerSec) {
        if (show_time) {
            String ts = MiscUtil.getCurrentTimeString();
            System.out.println(ts + ":" + name + ":" + totPerSec + ":" + intervalPerSec + ":" + cnt);
        } else {
            System.out.println(name + ":" + totPerSec + ":" + intervalPerSec + ":" + cnt);
        }
    }

    protected void showCount(String name, Long cnt, String totPerSec, String intervalPerSec) {
        if (show_time) {
            String ts = MiscUtil.getCurrentTimeString();
            System.out.println(ts + ":" + name + ":" + totPerSec + ":" + intervalPerSec + ":" + cnt);
        } else {
            System.out.println(name + ":" + totPerSec + ":" + intervalPerSec + ":" + cnt);
        }

    }

    @Override
    public boolean doInitialization() {
        return true;
    }

    private long interval = Long.MAX_VALUE;
    private long counter = 0L;
    private boolean decimals = false;
    protected boolean initialized = false;
    private long start_time = 0L;
    private long interval_time = 0L;
    private boolean show_time = true;
    private boolean do_notify = false;
    protected String name = "monitor";

}
