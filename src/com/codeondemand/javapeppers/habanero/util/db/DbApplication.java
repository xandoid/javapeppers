package com.codeondemand.javapeppers.habanero.util.db;

import com.codeondemand.javapeppers.habanero.util.misc.LoginApplication;
import org.apache.logging.log4j.LogManager;

public class DbApplication extends LoginApplication {

    public DbApplication() {

    }

    public void initialize(String[] args) {

        super.initialize();

        dburl = getProperty("db.url");
        dbdriver = getProperty("db.driver");

        conn_count = Integer.valueOf(getProperty("connection.count")).intValue();

        if (dburl == null || dbdriver == null) {
            // Create a database access manager
            db_mgr = new DbAccessManager(dburl, conn_count, uid, pwd);
            db_mgr.registerDriver(dbdriver);
        } else {
            logger.error("Connection information incomplete:" + dbdriver + ":" + dburl);
        }

    }

    /**
     * Supplied for testing
     *
     * @param args None needed for testing.
     */
    public static void main(String[] args) {
        DbApplication foo = new DbApplication();
        foo.initialize();
    }

    //protected Properties properties = null;
    private int conn_count = 1;
    protected DbAccessManager db_mgr = null;
    protected String dburl = null;
    protected String dbdriver = null;

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("DbApplication");

}
