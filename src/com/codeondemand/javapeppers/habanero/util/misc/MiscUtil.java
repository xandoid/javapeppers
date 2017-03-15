package com.codeondemand.javapeppers.habanero.util.misc;

import com.codeondemand.javapeppers.habanero.HabaneroMessages;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.logging.log4j.LogManager;

import javax.xml.transform.stream.StreamSource;
import java.io.*;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.*;

public class MiscUtil {

    private static Base64 urlSafeCodec = new Base64(true);
    private static Base64 secondCodec = new Base64(false);
    private static Hex hexCodec = new Hex();

    private static HashMap<Character, Character> map = new HashMap<Character, Character>();
    private static MessageDigest mMessageDigest = null;

    /**
     * Returns the base name of a class stripping out the package name.
     *
     * @return The name of the class without the package name.
     */
    public static String getBaseClassName(Object c) {
        String retval = null;
        if (c != null) {
            String name = c.getClass().getSimpleName();
            int idx = name.lastIndexOf('.');
            if (idx > 0) {
                retval = name.substring(idx + 1);
            } else {
                retval = name;
            }
        } else {
            logger.error(HabaneroMessages.getString("MiscUtil.0")); //$NON-NLS-1$
        }
        return retval;
    }

    /**
     * This method is just intended to bring a single line input in from the
     * console. Actually it is really more general purpose than that since you
     * provide the input stream, although it is really intended for simple
     * single line of input, so beware if you pass in a response file since it
     * will only read a single line from the file.
     *
     * @param in     The input stream to read from. If this is null, then System.in
     *               will be assumed.
     * @param prompt The prompt string to pass to the user if the input stream is
     *               the console or null (in which case the console is assumed).
     */
    public static String getConsoleInput(InputStream in, String prompt) {
        String ret = ""; //$NON-NLS-1$
        InputStream in_local = in;
        try {
            // Output a prompt if we are using the default console input
            if (in_local == null || in_local == System.in) {
                System.out.print(prompt);
                in_local = System.in;
            }
            BufferedReader rdr = new BufferedReader(new InputStreamReader(in_local));
            if (rdr != null) {
                while (ret != null && ret.length() == 0) {
                    ret = rdr.readLine();
                }

            }
        } catch (IOException ioe) {
            logger.error(ioe.toString());
        }
        return ret;
    }

    /**
     * This method is just intended to bring a single line input in from the
     * console. Unlike the getConsoleInput method, this method obscures the
     * input by echoing asterisk symbols to the screen. Actually it is really
     * more general purpose than that since you provide the input stream,
     * although it is really intended for simple single line of input, so beware
     * if you pass in a response file since it will only read a single line from
     * the file.
     */
    public static String getConsoleInputMasked(InputStream in, String prompt) {

        String ret = ""; //$NON-NLS-1$

        // Request the user to supply a password string
        try {
            char[] p = PasswordField.getPassword(new PushbackInputStream(System.in), prompt);
            if (p != null) {
                ret = String.valueOf(p);
            }
        } catch (IOException ioe) {
            logger.error(ioe.toString());
        }
        return ret;
    }

    /**
     * This method loaded a standard XML formatted properties file and returns a
     * Properties object for use by an application.
     *
     * @param f Specify the filename of the property file
     * @return A Properties object created from the specified file
     */
    public static Properties loadXMLPropertiesFile(String f) {

        Properties ret = null;
        try {
            String filename = new File("./", f).toString(); //$NON-NLS-1$

            StreamSource source = new StreamSource(ClassLoader.getSystemResourceAsStream(filename));
            if (source != null && (source.getInputStream() != null)) {
                ret = new Properties();
                ret.loadFromXML(source.getInputStream());
            } else {
                logger.error(HabaneroMessages.getString("MiscUtil.4") + //$NON-NLS-1$
                        filename);
            }
            return ret;
        } catch (IOException ioe) {
            logger.error(ioe.toString());
            ret = null;
        }

        return ret;
    }

    /**
     * Requests input for the value of a user id. If the input stream is null
     * then the console (System.in) will be assumed.
     *
     * @param in     The input stream to use for prompting. If this is null, then
     *               System.in will be used (typically console).
     * @param prompt The string to use for prompting the user. You can set this to
     *               null if you are reading from a stream that does not need a
     *               prompt.
     * @return Returns the string representation of the password.
     */
    public static String getUID(InputStream in, String prompt) {
        String retval = null;
        if (in == null) {
            retval = getConsoleInput(System.in, prompt);
        } else {
            retval = getConsoleInput(in, prompt);
        }
        return retval;
    }

    /**
     * Requests input for the value of a password. If the input stream is the
     * console, then the keyboard input will be masked for security reasons.
     *
     * @param in     The input stream to use for prompting. If this is null, then
     *               System.in will be used (typically console).
     * @param prompt The string to use for prompting the user. You can set this to
     *               null if you are reading from a stream that does not need a
     *               prompt.
     * @return Returns the string representation of the password.
     */
    public static String getPWD(InputStream in, String prompt) {
        String retval = null;
        if (in == null || in == System.in) {
            retval = getConsoleInputMasked(in, prompt);
        } else {
            retval = getConsoleInput(in, prompt);
        }
        return retval;
    }

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("MiscUtil");

    /**
     * Reads a file into a string. As each line is read, it is trimmed and then
     * a single blank space is added to the end of the line. The file will be
     * searched for in the classpath so you need only provide a name relative to
     * some directory that has been specified in the classpath.
     *
     * @param filename The file to read.
     * @return A single string with the concatenated contents of the specified
     * file.
     */
    public static String fileToString(String filename) {

        String ret = null;
        try {
            URL foo = ClassLoader.getSystemResource(filename);
            if (foo != null) {

                try (java.io.InputStream sqlcfg = foo.openStream()) {
                    try (BufferedReader sqlrdr = new BufferedReader(new java.io.InputStreamReader(sqlcfg))) {
                        StringBuffer querysb = new StringBuffer();
                        while (sqlrdr.ready()) {
                            String temp = sqlrdr.readLine();
                            if (temp != null) {
                                querysb.append(temp.trim() + " "); //$NON-NLS-1$
                            }
                        }
                        ret = querysb.toString();
                        sqlrdr.close();
                    }
                }
            } else {
                logger.error("Unable to load file:" + filename);
            }

        } catch (IOException ioe) {
            logger.error(ioe.toString());
        } catch (NullPointerException npe) {
            logger.error(HabaneroMessages.getString("MiscUtil.6") + filename); //$NON-NLS-1$
        }

        return ret;
    }

    /**
     * Reads a file into a string. As each line is read, it is trimmed and then
     * a single blank space is added to the end of the line. The file will be
     * searched for in the classpath so you need only provide a name relative to
     * some directory that has been specified in the classpath.
     *
     * @param filename The file to read.
     * @return A single string with the concatenated contents of the specified
     * file.
     */
    public static ArrayList<String> fileToArrayList(String filename, int size) {

        ArrayList<String> ret = null;
        try (java.io.InputStream sqlcfg = ClassLoader.getSystemResource(filename).openStream()) {
            try (BufferedReader sqlrdr = new BufferedReader(new java.io.InputStreamReader(sqlcfg))) {
                ret = new ArrayList<String>(size);
                while (sqlrdr.ready()) {
                    String temp = sqlrdr.readLine();
                    if (temp != null) {
                        ret.add(temp.trim());
                    }
                }
                sqlrdr.close();
            }

        } catch (IOException ioe) {
            logger.error(ioe.toString());
        } catch (NullPointerException npe) {
            logger.error(HabaneroMessages.getString("MiscUtil.7") + filename); //$NON-NLS-1$
        }

        return ret;
    }

    /**
     * Given a String representation of a data type such as character, integer,
     * etc. this will return the java.sql.Types values for that. It is a
     * convenience function that should be reasonably accurate.
     *
     * @param s The String specifying the type
     * @return A java.sql.Types value such as java.sql.Types.CHAR
     */
    public static int getSQLType(String s) {
        int retval = java.sql.Types.OTHER;
        if (s != null) {
            if (s.equalsIgnoreCase("char") || s.equalsIgnoreCase("varchar") //$NON-NLS-1$ //$NON-NLS-2$
                    || s.equalsIgnoreCase("character")) { //$NON-NLS-1$
                retval = java.sql.Types.CHAR;
            } else if (s.equalsIgnoreCase("int") //$NON-NLS-1$
                    || s.equalsIgnoreCase("smallint") //$NON-NLS-1$
                    || s.equalsIgnoreCase("integer")) { //$NON-NLS-1$
                retval = java.sql.Types.INTEGER;
            } else if (s.equalsIgnoreCase("DATE")) { //$NON-NLS-1$
                retval = java.sql.Types.DATE;
            } else if (s.equalsIgnoreCase("timestamp")) { //$NON-NLS-1$
                retval = java.sql.Types.TIMESTAMP;
            } else if (s.equalsIgnoreCase("decimal")) { //$NON-NLS-1$
                retval = java.sql.Types.DECIMAL;
            } else if (s.equalsIgnoreCase("double")) { //$NON-NLS-1$
                retval = java.sql.Types.DOUBLE;
            }
        }
        return retval;

    }

    /**
     * This method does a simple Base64 decoding (actually it does it in two
     * passes. The first is using a normal one and the second is a URL safe
     * pass.
     *
     * @param input The input string.
     * @return The decoded string.
     */
    public static synchronized String decodeB64String(String input) {
        String retval = null;
        try {
            retval = new String(secondCodec.decode(input.getBytes()));
            retval = new String(urlSafeCodec.decode(retval.getBytes()), "UTF-8"); //$NON-NLS-1$
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return retval;
    }

    public static String decodeSimpleB64String(String input) {
        String retval = null;
        try {
            retval = new String(urlSafeCodec.encode(input.getBytes()), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return retval;
    }

    /**
     * This method does a simple Base64 encoding (actually it does it in two
     * passes. The first is using a URL save pass and the second is a normal
     * pass.
     *
     * @param input The input string.
     * @return The encoded string.
     */
    public static String encodeB64String(String input) {
        String retval = null;
        try {
            retval = new String(urlSafeCodec.encode(input.getBytes()), "UTF-8"); //$NON-NLS-1$
            retval = new String(secondCodec.encode(retval.getBytes()));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return retval;
    }

    public static byte[] encodeHexBytes(byte[] input) {
        byte[] retval = null;
        retval = hexCodec.encode(input); // $NON-NLS-1$
        return retval;
    }

    public static byte[] decodeHexBytes(byte[] input) {
        byte[] retval = null;
        try {
            retval = hexCodec.decode(input);
        } catch (DecoderException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } // $NON-NLS-1$
        return retval;
    }

    /**
     * Does a string substitution that really is just a convenience method to do
     * check for null pointers and do a global string replacement.
     *
     * @param input The sting to change.
     * @param token The substring to look for
     * @param value The replacement value
     * @return The converted String.
     */
    public static String replaceToken(String input, String token, String value) {
        String retval = new String(input);

        if (retval != null && (token != null) && (value != null) && retval.contains(token.subSequence(0, token.length()))) {
            retval = retval.replaceAll(token, value);
        }

        return retval;
    }

    public static ArrayList<String> StringToList(String input, String delim) {
        ArrayList<String> retval = null;
        StringTokenizer stok = new StringTokenizer(input, delim);
        if (stok.hasMoreTokens()) {
            retval = new ArrayList<String>();
            while (stok.hasMoreTokens()) {
                retval.add(stok.nextToken().trim());
            }
        }
        return retval;
    }

    /**
     * This method looks at a string that my have embedded tokens that are
     * marked on both ends by some delimiter and attempts to replace those
     * tokens with the values of the tokens from a Properties object.
     *
     * @param p     A Properties object that may contain the tokens that are
     *              embedded in the string as keys.
     * @param in    The input string.
     * @param delim The token delimiter that is potentially in the string.
     * @return Returns a String that has had the tokens substituted.
     */
    public static String mapString(Properties p, String in, String delim) {
        String retval = ""; //$NON-NLS-1$
        // special case for db select statements
        if (!in.toUpperCase().trim().startsWith("SELECT")) {
            StringTokenizer stok = new StringTokenizer(in, delim);
            while (stok.hasMoreTokens()) {
                String tok = stok.nextToken();
                if (p.containsKey(tok)) {
                    retval = retval + p.getProperty(tok);
                } else {
                    retval = retval + tok;
                }
            }
        } else {
            retval = in;
        }

        return retval;
    }

    public static String buildPathFromHash(String hash, String os) {
        String retval = null;
        // Length of characters to process
        int l = 5;

        // Base64 b64 = new Base64(true);
        // String filename = b64.encodeToString(hash.substring(0,10).getBytes()
        // );
        String filename = hash;
        if (map.isEmpty()) {
            map.put('A', 'A');
            map.put('B', 'B');
            map.put('C', 'C');
            map.put('D', 'D');
            map.put('E', 'E');
            map.put('F', 'F');
            map.put('G', 'G');
            map.put('H', 'H');
            map.put('I', 'I');
            map.put('J', 'A');
            map.put('K', 'B');
            map.put('L', 'C');
            map.put('M', 'D');
            map.put('N', 'E');
            map.put('O', 'F');
            map.put('P', 'G');
            map.put('Q', 'H');
            map.put('R', 'I');
            map.put('S', 'A');
            map.put('T', 'B');
            map.put('U', 'C');
            map.put('V', 'D');
            map.put('W', 'E');
            map.put('X', 'F');
            map.put('Y', 'G');
            map.put('Z', 'H');
            map.put('0', 'I');
            map.put('1', 'A');
            map.put('2', 'B');
            map.put('3', 'C');
            map.put('4', 'D');
            map.put('5', 'E');
            map.put('6', 'F');
            map.put('7', 'G');
            map.put('8', 'H');
            map.put('9', 'I');
            map.put(',', 'A');
            map.put('+', 'B');
            map.put('/', 'C');

        }

        // Get the first 'l' characters in uppercase - skip the first
        // character since that will be the routing letter for
        // initial partitioning.
        String temp = hash.toUpperCase().substring(1, l + 1);

        char[] chars = new char[l * 3];
        int level = 0;
        char[] suffix = {'1', '2', '3', '4', '5'};
        for (int i = 0; i < l * 3; i = i + 3) {
            chars[i] = temp.charAt(level);
            if (map.containsKey(chars[i])) {
                chars[i] = map.get(chars[i]);
            } else {
                chars[i] = chars[i];
            }
            chars[i + 1] = suffix[level];
            if (os.equals("linux")) {
                chars[i + 2] = '/';
            } else {
                chars[i + 2] = '\\';
            }
            level++;
        }

        retval = String.copyValueOf(chars).trim() + filename.trim();
        return retval;
    }

    private static MessageDigest getMessageDigest() {
        MessageDigest retval = null;

        try {
            if (mMessageDigest == null) {
                retval = MessageDigest.getInstance("SHA-256");
            } else {
                retval = mMessageDigest;
            }
        } catch (NoSuchAlgorithmException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return retval;
    }

    public static String hashIMEI(String imei) {

        String retval = null;
        MessageDigest md = getMessageDigest();
        md.update(imei.getBytes());
        byte[] digest = md.digest();

        // Base64 b = new Base64(false);
        Hex h = new Hex();
        retval = new String(h.encode(digest));
        return retval;
    }

    public static boolean isCheckDigitValid(String imei, int cdigit) {

        boolean retval = false;

        if (cdigit == calcLuhnCheckDigit(imei)) {
            retval = true;
        }
        return retval;
    }

    public static boolean validateIMEI(String imei) {
        boolean retval = false;
        try {
            if (imei.length() == 15) {
                retval = MiscUtil.isCheckDigitValid(imei.substring(0, 14), Integer.parseInt(imei.substring(14)));
            }
        } catch (Exception e) {
            System.err.println(imei);
        }

        return retval;
    }

    /**
     * This method calculates the proper check digit for an IMEI number
     * expressed as a 14 digit numeric string (not HEX).
     * <p>
     * The Luhn algorithm is used to calculated the check digit as follows:
     * <p>
     * 1) Starting at the rightmost digit, double that digit and if it is 10 or
     * greater, then add the individual digits to our checksum. (for example 7*2
     * =14 then you would add 1+4 to the running total. 2) For the next digit,
     * just add to the checksum. 3) Alternate between doubling the digit and not
     * doubling the digit before adding to the running total. 4) When you have
     * processed all digits, then multiple by 9 and take the modulus 10 of that
     * number ( for example: if checksum was 62 then 62*9= 558 so the check
     * digit would be 558 modulo 10 which would be 8.
     *
     * @param imei - A 14 characters string consisting of numeric digits (0-9).
     * @return The proper check digit according to the Luhn algorithm.
     */
    public static int calcLuhnCheckDigit(String imei) {
        int retval = -1;

        char[] foo = imei.toCharArray();

        int sum = 0;
        boolean dbldigit = true;
        try {
            for (int i = foo.length - 1; i >= 0; i--) {
                if (dbldigit) {
                    int n = Integer.parseInt(String.valueOf(foo[i]));
                    n = n * 2;
                    if (n > 9) {
                        char[] bar = new Integer(n).toString().toCharArray();
                        sum += Integer.parseInt(String.valueOf(bar[0]));
                        sum += Integer.parseInt(String.valueOf(bar[1]));
                    } else {
                        sum += n;
                    }
                } else {
                    sum += Integer.parseInt(String.valueOf(foo[i]));
                }
                dbldigit = !dbldigit;
            }
            sum = sum * 9;
            retval = sum % 10;
        } catch (Exception e) {
            System.out.println(imei);
        }
        return retval;
    }

    public static String getCurrentDateString() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return sdf.format(new GregorianCalendar().getTime());
    }

    public static String getOffsetDateString(int days) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        long foo = new GregorianCalendar().getTimeInMillis() + (days * 24L * 60L * 60L * 1000L);
        GregorianCalendar bar = new GregorianCalendar();
        bar.setTimeInMillis(foo);
        return sdf.format(bar.getTime());
    }

    public static String getCurrentTimeString() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(new GregorianCalendar().getTime());
    }
}
