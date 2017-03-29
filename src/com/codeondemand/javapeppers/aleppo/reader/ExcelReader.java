/**
 *
 */
package com.codeondemand.javapeppers.aleppo.reader;

import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellValue;
import org.apache.poi.ss.usermodel.FormulaEvaluator;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

public class ExcelReader extends SourceReader implements FilenameFilter {

    @Override
    public boolean close() {
        try {
            wbfile.close();
        } catch (IOException e) {
            logger.error(e.toString());
        }
        return false;
    }

    @Override
    public Object read() {
        Object retval = null;
        if (sh != null) {
            HSSFRow row = sh.getRow(rowOffset);
            if (skipEmpty) {
                while (row != null && isEmpty(row)) {
                    rowOffset++;
                    row = sh.getRow(rowOffset);
                }
            }

            if (row == null) {
                retval = null;
            } else {
                if (testRow(row)) {
                    RecordCapsule rc = new RecordCapsule(workbook + ":" + worksheet + ":" + recordcount, null);
                    retval = rc;
                    if (colCount > 0 && colList == null) {
                        for (int i = colOffset; i < colOffset + colCount; i++) {
                            Object data = row.getCell(i).toString();
                            rc.addDataCapsule(new DataCapsule("cell:" + rowOffset + ":" + i, data), false);
                        }
                    } else if (colList != null) {
                        Iterator<Integer> it = colList.iterator();
                        int idx = 0;
                        while (it.hasNext()) {
                            int i = it.next().intValue();
                            CellValue data = null;
                            HSSFCell c = row.getCell(i);
                            String value = "";
                            if (c != null) {
                                if (c.getCellType() == Cell.CELL_TYPE_FORMULA) {
                                    data = evaluator.evaluate(c);
                                    if (data != null) {
                                        if (data.getCellType() == Cell.CELL_TYPE_STRING) {
                                            value = data.getStringValue();
                                        }
                                        if (data.getCellType() == Cell.CELL_TYPE_NUMERIC) {
                                            value = new Double(data.getNumberValue()).toString();
                                        }
                                    }
                                } else {
                                    value = row.getCell(i).toString();
                                }
                            }
                            rc.addDataCapsule(new DataCapsule(colNames.get(idx++), value), false);
                        }
                    } else {
                        int i = colOffset;
                        while (row.getCell(i) != null) {
                            Object data = row.getCell(i).toString();
                            rc.addDataCapsule(new DataCapsule("cell:" + rowOffset + ":" + i, data), false);
                            i++;
                        }
                    }
                }
                rowOffset++;
            }
        } else {
            logger.error("Null Sheet object, cannot read worksheet from workbook: " + workbook);
        }
        return retval;
    }

    protected boolean isEmpty(HSSFRow row) {
        boolean retval = true;
        if (colCount > 0 && row != null) {
            for (int i = 0; i < colCount; i++) {
                if (row.getCell(i) != null && row.getCell(i).toString() != null && row.getCell(i).toString().trim().length() > 0) {
                    retval = false;
                    break;
                }
            }
        } else {
            if (row != null && row.getCell(0) != null && row.getCell(0).toString().length() > 0) {
                retval = false;
            }
        }
        return retval;
    }

    protected boolean testRow(HSSFRow row) {
        boolean retval = true;
        if (exitTest == null) {
            if (!colList.isEmpty()) {
                boolean allempty = true;
                Iterator<Integer> it = colList.iterator();
                while (it.hasNext()) {
                    if (row.getCell(it.next()) != null) {
                        allempty = false;
                    }
                }
                retval = !allempty;
            } else if (row.getCell(colOffset) == null) {
                retval = false;
            }
        } else {
            StringTokenizer strtok = new StringTokenizer(exitTest, "|");
            int type = Integer.parseInt(strtok.nextToken());
            String test = strtok.nextToken();
            switch (type) {
                case 1:
                    if (row.getCell(colOffset).toString().startsWith(test)) {
                        retval = false;
                    }
                    break;
            }
        }
        return retval;
    }

    @Override
    public boolean reset() {
        try {
            wbfile.close();
            doInitialization();
        } catch (IOException e) {
            logger.error(e.toString());
        }
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
                    temp = MiscUtil.mapString(props, temp, "%");
                    findFile(temp);
                }
            }
            worksheet = props.getProperty(pfx + "worksheet");

            System.out.println(">" + workbook + "<:>" + worksheet + "<");
            try {
                wbfile = new FileInputStream(workbook);
                wb = new HSSFWorkbook(wbfile);
                evaluator = wb.getCreationHelper().createFormulaEvaluator();
                if (worksheet == null || worksheet.trim().length() == 0) {
                    sh = wb.getSheetAt(0);
                } else {
                    sh = wb.getSheet(worksheet);
                }
                if (props.getProperty(pfx + "row.offset") != null) {
                    rowOffset = Integer.parseInt(props.getProperty(pfx + "row.offset"));
                }
                if (props.getProperty(pfx + "col.offset") != null) {
                    colOffset = Integer.parseInt(props.getProperty(pfx + "col.offset"));
                }
                if (props.getProperty(pfx + "col.count") != null) {
                    colCount = Integer.parseInt(props.getProperty(pfx + "col.count"));
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

            } catch (FileNotFoundException e) {
                retval = false;
                logger.error(e.toString());
            } catch (IOException e) {
                retval = false;
                logger.error(e.toString());
            }

        } else {
            logger.error("No properties provided to ExcelReader");
        }

        // if (pmap.containsKey(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_FILE)) {
        // Properties p = MiscUtil.loadXMLPropertiesFile((String) pmap
        // .get(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_FILE));
        // if (p != null && p.getProperty("workbook") != null) {
        // workbook = p.getProperty("workbook");
        // }
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

    FormulaEvaluator evaluator = null;
    private FileInputStream wbfile = null;
    private HSSFWorkbook wb = null;
    private HSSFSheet sh = null;
    private String workbook = null;
    private String worksheet = null;

    @SuppressWarnings("unused")
    private int currentRow = -1;
    private int rowOffset = 0;
    private int colOffset = 0;
    private int colCount = -1;

    @SuppressWarnings("unused")
    private int rowCount = -1;

    @SuppressWarnings("unused")
    private boolean colHeader = false;
    private int recordcount = 1;
    private String exitTest = null;
    private boolean skipEmpty = false;
    private ArrayList<Integer> colList = null;
    private ArrayList<String> colNames = null;
    private String wbpattern = null;

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("ExcelReader");

}
