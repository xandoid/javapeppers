package com.codeondemand.javapeppers.habanero.util.db;

import org.apache.logging.log4j.LogManager;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.GregorianCalendar;
import java.util.TreeMap;

/**
 * This class contains some simple convenience methods that could be useful in
 * java.sql related code.
 *
 * @author gfa / gary_anderson@cz.javapeppers.com
 */
public class DbUtil {

    /**
     * Creates a TreeMap object that contains the column names for a result set
     * along with a integer identifier of the type. The type should correspond
     * to the constants defined in java.sql.Types class.
     *
     * @param rsmd The ResultSetMetaData instance that we want to build into map
     *             of column name -> type
     * @return Returns a TreeMap object with column names for keys and
     * java.sql.Types members as values
     */
    public static TreeMap<String, Integer> buildTypeTable(ResultSetMetaData rsmd) {

        TreeMap<String, Integer> retval = null;
        if (rsmd != null) {
            retval = new TreeMap<String, Integer>();

            try {
                int c = rsmd.getColumnCount();
                for (int i = 1; i <= c; i++) {
                    int type = rsmd.getColumnType(i);
                    String name = rsmd.getColumnName(i);
                    retval.put(name, new Integer(type));
                }
            } catch (SQLException e) {
                logger.error(e.toString());
            }
        }
        return retval;
    }

    public static TreeMap<String, Integer> buildTypeScaleTable(ResultSetMetaData rsmd) {

        logger.debug("Building a scale table for the result set columns");
        TreeMap<String, Integer> retval = null;
        if (rsmd != null) {
            retval = new TreeMap<String, Integer>();

            try {
                int c = rsmd.getColumnCount();
                for (int i = 1; i <= c; i++) {
                    int scale = rsmd.getScale(i);
                    String name = rsmd.getColumnName(i);
                    retval.put(name, new Integer(scale));
                    logger.debug("Setting scale for " + name + " to " + scale);
                }
            } catch (SQLException e) {
                logger.error(e.toString());
            }
        }
        return retval;
    }

    public static TreeMap<String, Integer> buildTypeSizeTable(ResultSetMetaData rsmd) {

        logger.debug("Building a size table for the result set columns");
        TreeMap<String, Integer> retval = null;
        if (rsmd != null) {
            retval = new TreeMap<String, Integer>();

            try {
                int c = rsmd.getColumnCount();
                for (int i = 1; i <= c; i++) {
                    int length = rsmd.getPrecision(i);
                    String name = rsmd.getColumnName(i);
                    retval.put(name, new Integer(length));
                    logger.debug("Setting length for " + name + " to " + length);
                }
            } catch (SQLException e) {
                logger.error(e.toString());
            }
        }
        return retval;
    }

    /**
     * Creates a TreeMap object that contains the column names for a result set
     * along with a String name for the type. T
     *
     * @param rsmd The ResultSetMetaData instance that we want to build into map
     *             of column name -> type
     * @return Returns a TreeMap object with column names for keys and type
     * names as values
     */
    public static TreeMap<String, String> buildTypeNameTable(ResultSetMetaData rsmd) {

        TreeMap<String, String> retval = null;
        if (rsmd != null) {
            retval = new TreeMap<String, String>();

            try {
                int c = rsmd.getColumnCount();
                for (int i = 1; i <= c; i++) {
                    String type = rsmd.getColumnTypeName(i);
                    String name = rsmd.getColumnName(i);
                    retval.put(name, type);
                }
            } catch (SQLException e) {
                logger.error(e.toString());
            }
        }
        return retval;
    }

    public static Timestamp parseTimestampString(String ts) {
        Timestamp retval = null;
        try {
            int year = Integer.parseInt(ts.substring(0, 4));
            int month = Integer.parseInt(ts.substring(5, 7)) - 1;
            int day = Integer.parseInt(ts.substring(8, 10));
            int hour = Integer.parseInt(ts.substring(11, 13));
            int minute = Integer.parseInt(ts.substring(14, 16));
            int second = Integer.parseInt(ts.substring(17, 19));
            retval = DbUtil.createTimestampYMDHMS(year, month, day, hour, minute, second);
        } catch (Exception e) {
            System.err.println(ts);
            e.printStackTrace();
        }

        return retval;
    }

    public static Time parseDB2TimeString(String ts) {
        Time retval = null;
        try {
            int year = Integer.parseInt(ts.substring(0, 4));
            int month = Integer.parseInt(ts.substring(5, 7)) - 1;
            int day = Integer.parseInt(ts.substring(8, 10));
            int hour = Integer.parseInt(ts.substring(11, 13));
            int minute = Integer.parseInt(ts.substring(14, 16));
            int second = Integer.parseInt(ts.substring(17, 19));
            retval = DbUtil.createTimeYMDHMS(year, month, day, hour, minute, second);
        } catch (Exception e) {
            System.err.println(ts);
            e.printStackTrace();
        }

        return retval;
    }

    /**
     * Creates a java.sql.Timestamp object for the current time.
     *
     * @return A java.sql.Timestamp object for NOW!
     */
    public static Timestamp currentTimestamp() {
        return new Timestamp(new GregorianCalendar().getTimeInMillis());
    }

    /**
     * Creates a java.sql.Timestamp object given a time in milliseconds)
     *
     * @param millis The number of milliseconds from 1/1/1970 to the time
     *               for which you need the Timestamp object.
     * @return A java.sql.Timestamp object.
     */
    public static Timestamp createTimestamp(long millis) {
        return new Timestamp(millis);
    }

    /**
     * Convenience method to build a java.sql.Timestamp object given a set of
     * paraemters that specify the year, month, and day
     *
     * @param year  An int specifying the year to use.
     * @param month An int specifying the month to use. (0-12)
     * @param day   An int specifying the day of month  (0-31)
     * @return A java.sql.Timestamp object.
     */
    public static Timestamp createTimestampYMD(int year, int month, int day) {
        return createTimestampYMDHMS(year, month, day, 0, 0, 0);
    }

    /**
     * Convenience method to build a java.sql.Timestamp object given a set of
     * paraemters that specify the year, month, day, hour,minute and second.
     *
     * @param year   An int specifying the year to use.
     * @param month  An int specifying the month to use. (0-12)
     * @param day    An int specifying the day of month  (0-31)
     * @param hour   An int specifying the hour of day. (0-23)
     * @param minute An int specifying the minute of the hour (0-59)
     * @param second An int specifying the second of the minute (0-59)
     * @return A java.sql.Timestamp object.
     */
    public static Timestamp createTimestampYMDHMS(int year, int month, int day, int hour, int minute, int second) {
        return new Timestamp(new GregorianCalendar(year, month, day, hour, minute, second).getTimeInMillis());
    }

    /**
     * Convenience method to build a java.sql.Timestamp object given a set of
     * paraemters that specify the year, month, day, hour,minute and second.
     *
     * @param year   An int specifying the year to use.
     * @param month  An int specifying the month to use. (0-12)
     * @param day    An int specifying the day of month  (0-31)
     * @param hour   An int specifying the hour of day. (0-23)
     * @param minute An int specifying the minute of the hour (0-59)
     * @param second An int specifying the second of the minute (0-59)
     * @return A java.sql.Timestamp object.
     */
    public static Time createTimeYMDHMS(int year, int month, int day, int hour, int minute, int second) {
        return new Time(new GregorianCalendar(year, month, day, hour, minute, second).getTimeInMillis());
    }

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("DbUtil");
}
