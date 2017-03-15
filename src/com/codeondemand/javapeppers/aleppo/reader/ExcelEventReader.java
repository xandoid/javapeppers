/**
 *
 */
package com.codeondemand.javapeppers.aleppo.reader;

import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import org.apache.logging.log4j.LogManager;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.FormulaEvaluator;

import java.io.File;
import java.io.FilenameFilter;
import java.util.*;

public class ExcelEventReader extends SourceReader implements FilenameFilter, Observer {

    @Override
    public boolean close() {
        return true;
    }

    public Object read() {
        Object retval = null;

        synchronized (lock2) {
            while (!ready) {
                Thread.yield();
            }
            if (finished) {
                return null;
            }
        }

        retval = readLine();
        ready = false;
        rdr.setContinue(true);
        return retval;
    }

    private Object readLine() {

        Object retval = null;
        Object[] row = rdr.getCurrentRow();
        RecordCapsule rc = new RecordCapsule(workbook + ":" + worksheet_number + ":" + recordcount, null);
        retval = rc;
        if (colCount > 0 && colList == null) {
            for (int i = colOffset; i < colOffset + colCount; i++) {
                Object data = row[i];
                rc.addDataCapsule(new DataCapsule("cell:" + rowOffset + ":" + i, data), false);
            }
        } else if (colList != null) {
            Iterator<Integer> it = colList.iterator();
            int idx = 0;
            while (it.hasNext()) {
                int i = ((Integer) it.next()).intValue();
                String value = "";
                if (row[i] != null) {
                    value = row[i].toString();
                }

                rc.addDataCapsule(new DataCapsule(colNames.get(idx++), value), false);
            }
        } else {
            int i = colOffset;
            while (row[i] != null) {
                Object data = row[i].toString();
                rc.addDataCapsule(new DataCapsule("cell:" + currentRow + ":" + i, data), false);
                i++;
            }
        }

        return retval;
    }

    protected boolean isEmpty(Object[] row) {
        boolean retval = true;
        return retval;
    }

    protected boolean testRow(Object[] row) {
        boolean retval = true;
        return retval;
    }

    @Override
    public boolean reset() {

        doInitialization();
        return false;
    }

    @Override
    public boolean doInitialization() {
        boolean retval = true;
        String pfx = "";
        if (pmap.containsKey("pfx")) {
            pfx = pmap.get("pfx").toString();
        }

        if (props != null) {
            if (props.containsKey(pfx + "workbook")) {
                workbook = props.getProperty(pfx + "workbook");
            } else {
                if (props.containsKey(pfx + "workbook.pattern") && props.containsKey(pfx + "workbook.dir")) {
                    wbpattern = props.getProperty(pfx + "workbook.pattern");
                    String temp = props.getProperty(pfx + "workbook.dir");
                    //temp = MiscUtil.mapString(props, temp, "%");
                    findFile(temp);
                }
            }
            worksheet_number = Integer.parseInt(props.getProperty(pfx + "worksheet_number"));

            System.out.println(workbook + ":" + worksheet_number);

            if (props.getProperty(pfx + "row.offset") != null) {
                rowOffset = Integer.parseInt(props.getProperty(pfx + "row.offset"));

            }
            if (props.getProperty(pfx + "col.offset") != null) {
                colOffset = Integer.parseInt(props.getProperty(pfx + "col.offset"));
            }
            if (props.getProperty(pfx + "col.maxCol") != null) {
                maxCol = Integer.parseInt(props.getProperty(pfx + "col.maxCol"));
            }
            if (props.getProperty(pfx + "row.count") != null) {
                rowCount = Integer.parseInt(props.getProperty(pfx + "row.count"));
            }
            if (props.getProperty(pfx + "col.header.exists") != null) {
                colHeader = Boolean.parseBoolean(props.getProperty(pfx + "col.header.exists"));
            }
            if (props.getProperty(pfx + "exit.test") != null) {
                exitTest = props.getProperty(pfx + "exit.test");
            }

            if (props.getProperty(pfx + "col.list") != null) {
                colList = new ArrayList<Integer>();
                colNames = new ArrayList<String>();
                String temp = props.getProperty(pfx + "col.list");
                StringTokenizer foo = new StringTokenizer(temp, "|");
                while (foo.hasMoreTokens()) {
                    StringTokenizer bar = new StringTokenizer(foo.nextToken(), ":");
                    colList.add(Integer.parseInt(bar.nextToken().trim()));
                    colNames.add(bar.nextToken());
                    colCount++;
                }
            }

            if (props.getProperty(pfx + "skip.empty.rows") != null) {
                skipEmpty = Boolean.parseBoolean(props.getProperty(pfx + "skip.empty.rows"));
            }
            rdr = new ExcelReaderEventModel();
            rdr.setStartRow(rowOffset);
            rdr.setLastColumn(maxCol);
            rdr.initializeModel(workbook, worksheet_number);
            Thread foo = new Thread(rdr);
            rdr.addObserver(this);
            foo.start();

        } else {
            logger.error("No properties provided to ExcelReader");
        }

        return retval;
    }

    public boolean accept(File arg0, String arg1) {
        boolean retval = false;
        if (arg1.startsWith(wbpattern)) {
            workbook = arg0.getAbsolutePath() + File.separatorChar + arg1;
            retval = true;
        }
        return retval;
    }

    private void findFile(String dir) {
        File foo = new File(dir);
        if (foo != null && foo.isDirectory()) {
            foo.listFiles(this);
        }
    }

    private ExcelReaderEventModel rdr = null;
    FormulaEvaluator evaluator = null;
    //private FileInputStream wbfile = null;

    @SuppressWarnings("unused")
    private HSSFWorkbook wb = null;

    @SuppressWarnings("unused")
    private HSSFSheet sh = null;
    private String workbook = null;
    private int worksheet_number = 0;
    private Integer currentRow = -1;
    private int rowOffset = 0;
    private int colOffset = 0;
    private int colCount = -1;
    private int maxCol = -1;
    @SuppressWarnings("unused")
    private int rowCount = -1;
    @SuppressWarnings("unused")
    private boolean colHeader = false;
    private int recordcount = 1;

    @SuppressWarnings("unused")
    private String exitTest = null;

    @SuppressWarnings("unused")
    private boolean skipEmpty = false;
    private ArrayList<Integer> colList = null;
    private ArrayList<String> colNames = null;
    private String wbpattern = null;
    private Boolean ready = false;
    private Object lock = new Object();
    private Object lock2 = new Object();

    @SuppressWarnings("unused")
    private Integer oldRow = -1;
    private Boolean finished = false;

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("ExcelEventReader");

    public void update(Observable arg0, Object arg1) {
        synchronized (lock) {
            currentRow++;
            if (arg1 instanceof Boolean) {
                Boolean temp = (Boolean) arg1;
                if (temp) {
                    ready = true;
                } else {
                    ready = true;
                    finished = true;
                }
            }
        }
    }

}
