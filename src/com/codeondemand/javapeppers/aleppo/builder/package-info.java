/**
 * The  com.codeondemand.javapeppers.aleppo.builder package is to allow us to separate
 * the formatting of the output from the actual output of the dataflow. 
 *
 * The classes provided standard in this package provide a broad set
 * of functionality.  This includes the following:
 * <br>
 * <ul>
 * <li>Delimited format with a choice of delimiters (comma,pipe,tab)
 * <li>Fixed format record builder.
 * <li>Simple conversion to an XML tagged format
 * <li>JSON formatting
 * <li>Generation of a DDL suitable to create a simple table for storing the data
 * <li>Null builder which passes the data flow object unchanged to the writer class.
 * </ul>
 * @author Gary Anderson
*/
package com.codeondemand.javapeppers.aleppo.builder;