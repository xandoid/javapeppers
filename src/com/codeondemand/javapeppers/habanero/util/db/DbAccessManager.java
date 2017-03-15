/**
 *
 */

package com.codeondemand.javapeppers.habanero.util.db;

import com.codeondemand.javapeppers.habanero.HabaneroMessages;
import org.apache.logging.log4j.LogManager;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * The DbAccessManager class is responsible for maintaining connections with
 * databases. It manages java.sql.Connection objects in a variable number of
 * queues gives them out upon request without any restrictions.  The pattern
 * is to give the connections out on a round-robin basis.
 *
 * @author gfa
 */

public class DbAccessManager {

    // ***********************************************************************
    // Constructors
    // ***********************************************************************

    /**
     * Instantiates a DbAccessManager which creates connections to a database
     * specified by the url parameter and creates the specifiec number of
     * connections to the database.
     *
     * @param url   The String containing a URL which specifies the database for
     *              which connection is desired. This is the default url for this
     *              DbAccessManager, but it can support multiple connections to
     *              multiple databases.
     * @param count The number of connection Queues to create for contact with the
     *              database. If multiple databases are managed, then 'count'
     *              connections will be created for each of the databases.
     */
    public DbAccessManager(String url, int count, String user, String password) {
        urlString = url;
        connectionCount = count;
        dbUser = user;
        dbPassword = password;
    }

    /**
     * Instantiates a DbAccessManager which creates connections to a database
     * specified by the url parameter and creates the specified number of
     * connections to the database.
     *
     * @param url   The String containing a URL which specifies the database for
     *              which connection is desired. This is the default url for this
     *              DbAccessManager, but it can support multiple connections to
     *              multiple databases.
     * @param count The number of connection Queues to create for contact with the
     *              database. If multiple databases are managed, then 'count'
     *              connections will be created for each of the databases.
     */
    public DbAccessManager(String url, int count) {
        urlString = url;
        connectionCount = count;
    }

    // ***********************************************************************
    // Public methods and data
    // ***********************************************************************

    /**
     * Returns an available Connection with the database, assuming the
     * connection is made with using the privilege of the user who is running
     * this application. Also assumes the URL used in the constructor.
     */
    public Connection getConnection() {
        return getConnection(urlString, dbUser, dbPassword);
    }

    /**
     * Returns an available connection for a specified user/password. Note that
     * the user/password is only used in the event that there is not already a
     * connection established for this URL. Connections to specific URLs are not
     * managed and/or distributed on a per user basis.
     *
     * @param url      The String containing a URL which specifies the database for
     *                 which connection is desired.
     * @param user     The database logon name for the request.
     * @param password The logon password for the request.
     */
    public synchronized Connection getConnection(String url, String user, String password) {
        dbUser = user;
        dbPassword = password;

        Connection ret = null;

        // The basic strategy here is to see if there is currently a queue
        // associated with the specified url. If not, the code creates and
        // initializes one. If there is a queue associated with this url then
        // a Connection object is removed from the front of the queue and
        // returned. Note, since we reuse connections, the Connection
        // object is also returned to the end of the queue, where it will be
        // available for later use. This strategy allows us to have multiple
        // database connections available for use.

        ConcurrentLinkedQueue<Connection> connectionQueue = connectionTable.get(url);
        if (connectionQueue != null) {
            try {
                // Note: There is some experimenting going on related to
                // the number of times to reuse a connection and how it
                // relates to memory management in the system. It appears that
                // this section will be removed and that connections will be
                // used for the duration of the server operation.
                if (connectionQueue.isEmpty()) {
                    if (user.equals("") && password.equals("")) { //$NON-NLS-1$ //$NON-NLS-2$
                        ret = DriverManager.getConnection(url);
                    } else {
                        ret = DriverManager.getConnection(url, user, password);
                    }
                    connectionQueue.add(ret);
                } else {
                    Connection tmp = connectionQueue.poll();
                    connectionQueue.add(tmp);
                    ret = tmp;
                }

            } catch (SQLException e2) {
                logger.error(e2.toString());
                e2.printStackTrace();
            }
        } else {
            connectionQueue = new ConcurrentLinkedQueue<Connection>();
            try {
                Connection temp = null;
                for (int i = 0; i < connectionCount; i++) {
                    if (user.equals("") && password.equals("")) { //$NON-NLS-1$ //$NON-NLS-2$
                        temp = DriverManager.getConnection(url);
                    } else {
                        //System.out.println(url+"  "+ user+"   "+password);
                        temp = DriverManager.getConnection(url, user, password);
                    }
                    connectionQueue.add(temp);
                    logger.debug(HabaneroMessages.getString("DbAccessManager.4") + temp.toString()); //$NON-NLS-1$
                }
                ret = temp;
                connectionTable.put(url, connectionQueue);
            } catch (SQLException e) {
                logger.error(e.toString());
                e.printStackTrace();
            }
        }

        return ret;
    }

    /**
     * Returns an available Connection with the database, assuming the
     * connection is made with using the privilege of the user who is running
     * this application.
     *
     * @param url The String containing a URL which specifies the database for
     *            which connection is desired.
     */
    public Connection getConnection(String url) {

        return getConnection(url, dbUser, dbPassword);

    }

    /**
     * This method attempts to load a Java class with the specified name. If
     * successful, the driver will be available for use by the DriverManager
     * class when needed. THIS METHOD MUST BE CALLED FOR EACH DRIVER which you
     * expect to be needed prior to any calls to the getConnection() methods.
     * <p>
     * String driver The String representing the classname of the database
     * driver you will be needing.
     */
    public void registerDriver(String driver) {
        try {
            Class.forName(driver);
            Enumeration<Driver> bar = DriverManager.getDrivers();
            if (!bar.hasMoreElements()) {
                logger.error(HabaneroMessages.getString("DbAccessManager.5")); //$NON-NLS-1$
            } else {
                while (bar.hasMoreElements()) {
                    Driver d = bar.nextElement();
                    //System.out.println( d.getClass().toString());
                    logger.debug(d.getClass().toString());
                }
            }
        } catch (ClassNotFoundException e) {
            logger.error(e.toString());
        }
    }

    // ***********************************************************************
    // Protected methods and data
    // ***********************************************************************

    // ***********************************************************************
    // Private data and methods
    // ***********************************************************************
    private String dbUser = new String(""); //$NON-NLS-1$
    private String dbPassword = new String(""); //$NON-NLS-1$

    private int connectionCount = 5;
    private String urlString = null;
    private HashMap<String, ConcurrentLinkedQueue<Connection>> connectionTable = new HashMap<String, ConcurrentLinkedQueue<Connection>>();

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("DbAccessManager");


}
