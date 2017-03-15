package com.codeondemand.javapeppers.habanero.util.misc;

import org.apache.logging.log4j.LogManager;

import java.util.*;

/**
 * The purpose of this class is to provide some generic functionality
 * to group together items that have extended relationships.  For
 * instance, if a is related to b, b is related to c, and c is related
 * to d, this class would build a single relationship group for
 * a,b,c and d.  If however c was not related to d, but d was related
 * to e, then two groups (a,b,c and d,e) would be built.
 * <p>
 * The process begins by adding all of the known associations in the
 * form of id pairs.  The current implementation expects the ids to
 * be strings, but this is simply changed.  For each relationship, one
 * call to addAssociation should be made.
 * <p>
 * Following all the calls to addAssociation, the next call is to
 * buildGroups.  This will return a TreeMap<Integer,HashSet<String>>
 * that contains all of the groups of related ids keyed by group number.
 * For this implementation groups are expected to be keyed by Integers
 * from 1 to n.  The keys are generated internally by the class.  The
 * application using the class can certainly map the group keys to
 * whatever series it desires.  Each group is a set of distinct ids.
 * Each id in the association universe will only be the member of a
 * single group.
 *
 * @author gfa
 */
public class BuildGroupingTable {

    /**
     * This method adds the individual takes two ids and adds them to the
     * respective association lists for each of the ids.  The intent is that
     * this function will be called repeatedly to build up a table of all of
     * the associations.  The HashMap storing the association information
     * needs to be
     *
     * @param id1 One of the ids to add to the association list. It will be
     *            added to the association list for id2.
     * @param id2 One of the ids to add to the association list. It will be
     *            added to the association list for id1.
     */
    public void addAssociations(String id1, String id2) {

        // Proceed if the parameters supplied are non-null
        if (id1 != null && id2 != null) {
            ArrayList<String> list1 = associationTbl.get(id1);
            ArrayList<String> list2 = associationTbl.get(id2);
            if (list1 == null) {
                list1 = new ArrayList<String>();
                associationTbl.put(id1, list1);
            }
            if (list2 == null) {
                list2 = new ArrayList<String>();
                associationTbl.put(id2, list2);
            }

            // Add ids to the appropriate lists. id1
            if (!list1.contains(id2)) {
                list1.add(id2);
            }
            if (!list2.contains(id1)) {
                list2.add(id1);
            }
        } else {
            logger.error("Invalid arguments passed to addAssociations function");
        }
    }

    /**
     * This function clears the accumulated association and group tables.
     * created up to this point.
     */
    public void reset() {
        associationTbl.clear();
        groupMap.clear();
    }

    /**
     * Call this function when you would like to get the grouping
     * results of the associations that you have specified up to this
     * point.  Note that each call to this function results in a
     * new build of the group table that is returned, so it can be
     * called at various stages as you add associations.
     *
     * @return A TreeMap<Integer,HashSet<String>> containing the
     * distinct association groups.
     */
    public TreeMap<Integer, HashSet<String>> buildGroups() {
        int group = 0;

        HashSet<String> currentSet = null;
        groupMap.clear();
        Iterator<String> it = associationTbl.keySet().iterator();
        while (it.hasNext()) {
            String id = it.next();
            currentSet = new HashSet<String>();
            if (!processed.contains(id)) {
                group++;
                currentSet.add(id);
                processGroup(id, currentSet);
                groupMap.put(group, currentSet);
            }
        }
        return groupMap;
    }

    /**
     * This is a recursive function that adds the ids that are associated with a
     * specified id to the current working set. For each id it adds to the
     * working set, it will recursively call itself to add the ids associated
     * with the new id.
     * <p>
     * Note: The starting id is also added to the current working set by this
     * function.
     *
     * @param idin The starting id for the association.
     */
    private void processGroup(String idin, HashSet<String> currentSet) {
        ArrayList<String> list = associationTbl.get(idin);
        if (list != null) {
            Iterator<String> it = list.iterator();
            while (it.hasNext()) {
                String id = it.next();
                currentSet.add(id);
                if (!processed.contains(id)) {
                    processed.add(id);
                    processGroup(id, currentSet);
                }
            }
        }
    }

    private HashMap<String, ArrayList<String>> associationTbl = new HashMap<String, ArrayList<String>>();
    private TreeMap<Integer, HashSet<String>> groupMap = new TreeMap<Integer, HashSet<String>>();

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("BuildGroupingTable");

    private HashSet<String> processed = new HashSet<String>();
}
