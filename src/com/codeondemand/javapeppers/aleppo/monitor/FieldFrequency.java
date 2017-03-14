/**
 * 
 */
package com.codeondemand.javapeppers.aleppo.monitor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;

import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;

public class FieldFrequency extends MonitorProcess {

	@Override
	public boolean initialize(RecordCapsule rc) {
		return true;
	}

	public void done() {
		BufferedWriter br = null;

		if (names == null) {
			logger.debug("No records accumulated by FieldFrequency monitor");
			return;
		}
		if (pmap.containsKey("file") && pmap.get("file") instanceof String) {
			String outputfile = (String) pmap.get("file");
			try {
				br = new BufferedWriter(new FileWriter(new File(outputfile)));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			br = new BufferedWriter(new OutputStreamWriter(System.out));
		}

		if (br != null) {
			// write out header
			try {
				br.write("field|non-null cnt|non-null pct|min|max|uniq|");
				for( int i = 0 ; i < uniqlimit ; i++){
					br.write("val_"+i+"|count_"+i+"|");
				}
				br.write("\n");
				logger.debug("names arraylist >" + names + "<");
				for (int i = 0; i < names.size(); i++) {
					long count = counters.get(i);
					double pct = count * 100.0 / maxcount;
					NumberFormat nf = NumberFormat.getPercentInstance();
					nf.setMaximumFractionDigits(2);
					nf.setMinimumFractionDigits(2);
					br.write(names.get(i) + "|" + counters.get(i) + "|"
							+ nf.format(pct / 100.0) + "|");
					String min = "null";
					if (minmap.get(names.get(i)) != null) {
						min = minmap.get(names.get(i)).toString();
						if (min.length() == 0) {
							min = "null";
						}
					}
					br.write(min);
					br.write("|");
					String max = "null";
					if (maxmap.get(names.get(i)) != null) {
						max = maxmap.get(names.get(i)).toString();
					}
					br.write(max);
					br.write("|");
					// if (posmap.get(names.get(i)) != null
					// && negmap.get(names.get(i)) != null) {
					// br.write(posmap.get(names.get(i)).toString());
					// br.write("|");
					// br.write(negmap.get(names.get(i)).toString());
					// }
					// br.write("|");
					if (uniqmap.containsKey(names.get(i))) {
						TreeMap<String, Long> temp = uniqmap.get(names.get(i));
						if (temp.size() == uniqlimit) {
							br.write("max");
						} else {
							br.write("" + temp.size());
						}
						if (temp.size() <= uniqlimit) {
							Iterator<String> k = temp.keySet().iterator();
							while (k.hasNext()) {
								String foo = k.next();
								br.write("|");
								if (foo.length() == 0) {
									br.write("null");
								} else {
									br.write(foo);
								}
								br.write("|");
								String val = temp.get(foo).toString();
								if (val.length() == 0) {
									val = "null";
								}
								br.write(val);
							}

						}
					}
					br.write("\n");
					br.flush();

				}
				br.close();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	public boolean doInitialization() {
		return true;
	}

	@Override
	protected synchronized boolean doMonitor(RecordCapsule input) {

		if (counters == null) {
			counters = new ArrayList<Long>();
			names = new ArrayList<String>();
			for (int i = 0; i < input.getFieldCount(); i++) {
				DataCapsule dc = input.getField(i);
				String name = dc.getName();
				if (input.getField(i).isNull()
						|| input.getField(i).getData() == null
						|| input.getField(i).getData().toString().length() == 0) {
					counters.add(0L);
				} else {
					counters.add(1L);
					doMinMax(name, dc);
					doPosNeg(name, dc);
					doUnique(name, dc);
				}
				names.add(input.getField(i).getName().trim());
			}
		} else {
			for (int i = 0; i < input.getFieldCount(); i++) {
				DataCapsule dc = input.getField(i);
				String name = input.getField(i).getName();
				if (!dc.isNull() && !input.getField(i).isNull()
						&& input.getField(i).getData() != null
						&& input.getField(i).getData().toString().length() != 0) {
					counters.set(i, counters.get(i) + 1);
					maxcount = maxcount > counters.get(i) ? maxcount : counters
							.get(i);
				}
				if (!dc.isNull()) {
					doMinMax(name, dc);
					doPosNeg(name, dc);
					doUnique(name, dc);
				}
			}
		}

		return true;
	}

	private void doUnique(String name, DataCapsule dc) {
		TreeMap<String, Long> temp = null;
		if (!uniqmap.containsKey(name)) {
			temp = new TreeMap<String, Long>();
			temp.put(dc.getData().toString().trim(), new Long(0));
			uniqmap.put(name, temp);
		} else {
			temp = uniqmap.get(name);
			if (temp.size() < uniqlimit) {
				if (!temp.containsKey(dc.getData().toString().trim())) {
					temp.put(dc.getData().toString().trim(), new Long(1L));
				} else {
					Long l = (Long) temp.get(dc.getData().toString().trim());
					l++;
					temp.put(dc.getData().toString().trim(), l);
				}
			}
		}

	}

	private void doPosNeg(String name, DataCapsule dc) {
		if (dc.getData() instanceof Number) {
			if (!negmap.containsKey(name)) {
				negmap.put(name, new Long(0));
			}
			if (!posmap.containsKey(name)) {
				posmap.put(name, new Long(0));
			}

			float temp = Float.parseFloat(dc.getData().toString());
			if (temp < 0.0) {
				long foo = negmap.get(name).longValue();
				// System.out.println("neg count:"+foo);
				negmap.put(name, new Long(foo + 1));
			} else if (temp > 0.0) {
				long foo = posmap.get(name).longValue();
				// System.out.println("pos count:"+foo);
				posmap.put(name, new Long(foo + 1));
			}
		}

	}

	private void doMinMax(String name, DataCapsule dc) {
		if (dc.getData() != null && dc.getData().toString().length() > 0) {

			// See the map for the first time
			if (!minmap.containsKey(name)) {
				minmap.put(name, dc.getData());
			}
			if (!maxmap.containsKey(name)) {
				maxmap.put(name, dc.getData());
			}

			if (dc.getData() instanceof Number
					&& minmap.get(name) instanceof Number) {
				Double foo = new Double(dc.getData().toString());
				Double bar = new Double(minmap.get(name).toString());
				if (foo <= bar) {
					minmap.put(name, foo);
				}
			} else if (dc.getData().toString()
					.compareTo(minmap.get(name).toString()) <= 0) {
				minmap.put(name, dc.getData());
			}

			if (dc.getData() instanceof Number
					&& maxmap.get(name) instanceof Number) {
				Double foo = new Double(dc.getData().toString());
				Double bar = new Double(maxmap.get(name).toString());
				if (foo > bar) {
					maxmap.put(name, foo);
				}
			} else if (dc.getData().toString()
					.compareTo(maxmap.get(name).toString()) > 0) {
				maxmap.put(name, dc.getData());
			}
		}
	}

	private Long maxcount = 0L;
	private TreeMap<String, TreeMap<String, Long>> uniqmap = new TreeMap<String, TreeMap<String, Long>>();
	private TreeMap<String, Object> minmap = new TreeMap<String, Object>();
	private TreeMap<String, Object> maxmap = new TreeMap<String, Object>();
	private TreeMap<String, Long> negmap = new TreeMap<String, Long>();
	private TreeMap<String, Long> posmap = new TreeMap<String, Long>();
	private ArrayList<String> names = null;
	private ArrayList<Long> counters = null;
	private int uniqlimit = 105;
	
	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("FieldFrequency");

}
