/**
 * The com.codeondemand.javapeppers.aleppo.flowcontrol package is a process control package that is
 * introduced as a layer on top of the normal Aleppo data flow functionality to
 * orchestrate complex data flows consisting of independent subflows.
 * <p>
 * Instead of 'data' flowing through the system, typically there will be a process
 * token flowing through the system (it could be a single token that is passed
 * through a series of subflows (also orchestrated by Aleppo)
 * <p>
 * This package uses a database to control and document the results of the process
 * as each of the subflow tasks are oriented.
 *
 * @author Gary Anderson
 */
package com.codeondemand.javapeppers.aleppo.flowcontrol;