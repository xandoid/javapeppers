package com.codeondemand.javapeppers.habanero.util.misc;

//import org.apache.commons.logging.LogFactory;

/**
 * The LoginApplication is base class for console based applications and which
 * provides only one bit of functionality, namely requesting the user to supply
 * a userid and password for use in logging in to some system.
 */
public class LoginApplication extends BaseApplication {

    public LoginApplication() {

    }

    /**
     * Prompts the user via the console for a user id and password.  Note
     * that the password string will be masked as it is entered. This will
     * default to using
     */
    public void initialize() {

        super.initialize();

        if (properties != null && properties.getProperty("do.login").equals("true")) {

            // Request the user to supply a userid string
            uid = MiscUtil.getConsoleInput(System.in, uidPrompt);
            pwd = MiscUtil.getConsoleInputMasked(System.in, pwdPrompt);

        }
    }

    public void initialize(boolean login) {
        if (login) {
            uid = MiscUtil.getConsoleInput(System.in, uidPrompt);
            pwd = MiscUtil.getConsoleInputMasked(System.in, pwdPrompt);
        }
    }

    /**
     * Allows overriding the default password prompt.
     *
     * @param p The string to use for the password prompt.
     */
    protected void setPwdPrompt(String p) {
        if (p != null) {
            pwdPrompt = new String(p);
        }
    }

    /**
     * Allows overriding the default userid prompt.
     *
     * @param p The string to use for the userid prompt.
     */
    protected void setUidPrompt(String p) {
        if (p != null) {
            uidPrompt = new String(p);
        }
    }

    protected String uidPrompt = new String("Enter user id: ");
    protected String pwdPrompt = new String("Enter password: ");

    protected String uid = null;
    protected String pwd = null;

    //private static org.apache.commons.logging.Log logger = LogFactory
    //		.getLog(LoginApplication.class);

    /**
     * Simple test function that request the userid and password once with the
     * default prompts, and once with prompts specified programmatically.
     * <p>
     * No arguments are expected.
     */
    public static void main(String args[]) {
        System.out.println("---------------------------------------------------");
        System.out.println("Running unmodified test:");
        System.out.println("---------------------------------------------------");
        LoginApplication la1 = new LoginApplication();
        la1.initialize();
        System.out.println("---------------------------------------------------");
        System.out.println("\tUser entered: " + la1.uid + ":" + la1.pwd);

        System.out.println("===================================================");
        System.out.println("---------------------------------------------------");
        System.out.println("Running test specifying prompts: (uid: and pwd:)");
        System.out.println("---------------------------------------------------");
        LoginApplication la2 = new LoginApplication();
        la2.setUidPrompt("uid:");
        la2.setPwdPrompt("pwd:");
        la2.initialize(true);
        System.out.println("---------------------------------------------------");
        System.out.println("\tUser entered: " + la2.uid + ":" + la2.pwd);
        System.out.println("===================================================");
    }
}
