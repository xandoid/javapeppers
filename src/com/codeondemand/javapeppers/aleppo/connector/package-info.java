/**
 * The com.codeondemand.javapeppers.aleppo.connector package provides the core classes that move the data
 * through the the data/process flow.
 * <p>
 * The RecordConnector class basically just has a simple function.  It requests a
 * payload record from a source, passes the payload to each of the processing nodes
 * and then passes the payload to one or more destinations.
 * <p>
 * The MultiplexConnector allows the additional functionality of pulling from
 * multiple sources, processing the payload through the processing nodes and then
 * delivering the payloads through to one or more destinations.
 *
 * @author Gary Anderson
 */
package com.codeondemand.javapeppers.aleppo.connector;