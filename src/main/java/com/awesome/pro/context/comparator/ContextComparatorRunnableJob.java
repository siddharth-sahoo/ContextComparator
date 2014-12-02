package com.awesome.pro.context.comparator;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.awesome.pro.context.comparator.references.ContextComparatorConfigReferences;
import com.awesome.pro.context.comparator.references.ContextComparatorMongoReferences;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;

/**
 * Runnable instance which compares a single name space
 * in a context.
 * @author balakrishnan.m
 */
public class ContextComparatorRunnableJob implements Runnable  {

	/**
	 * Root logger instance.
	 */
	private static final Logger LOGGER = Logger.getLogger(
			ContextComparatorRunnableJob.class);

	/**
	 * Configurations retrieved from MongoDB.
	 */
	private final BasicDBObject config;

	/**
	 * Name of the name space under test.
	 */
	private String namespace;

	/**
	 * @param obj Mappings from MongoDB.
	 */
	ContextComparatorRunnableJob(final BasicDBObject obj) {
		config = obj;
	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		namespace = config.getString(
				ContextComparatorMongoReferences.FIELD_NAME_SPACE);

		final Map<String,Map<String,String>> contextData1 = 
				ContextComparator.CONTEXT_1.
				getContextData(namespace);
		final Map<String,Map<String,String>> contextData2 = 
				ContextComparator.CONTEXT_2.
				getContextData(config.getString(
						ContextComparatorMongoReferences.FIELD_NAME_SPACE));

		boolean flag = false;
		if (contextData1 == null) {
			LOGGER.error("Context 1 data is null.");
			ContextComparator.addResult(namespace,
					ContextComparatorConfigReferences.TEST_CATEGORY_SANITY,
					ContextComparatorConfigReferences.TEST_STATUS_FAILED,
					"Not found in " + ContextComparator.CONTEXT_1.getName());
			flag = true;
		}

		if (contextData2 == null) {
			LOGGER.error("Context 2 data is null.");
			ContextComparator.addResult(namespace,
					ContextComparatorConfigReferences.TEST_CATEGORY_SANITY,
					ContextComparatorConfigReferences.TEST_STATUS_FAILED,
					"Not found in " + ContextComparator.CONTEXT_2.getName());
			flag = true;
		}

		// Sanity check.
		if (flag) {
			return;
		} else {
			ContextComparator.addResult(namespace,
					ContextComparatorConfigReferences.TEST_CATEGORY_SANITY,
					ContextComparatorConfigReferences.TEST_STATUS_PASSED,
					null);
		}

		// Count of rows is inconclusive.
		checkRowCount(contextData1.size(), contextData2.size());

		// Key set comparison yields common keys.
		final Set<String> commonKeys = checkKeySet(contextData1.keySet(),
				contextData2.keySet());

		boolean testResult = true;
		final Iterator<String> iter = commonKeys.iterator();
		while (iter.hasNext()) {
			final String key = iter.next();
			final boolean temp = checkRow(contextData1.get(key),
					contextData2.get(key), key);
			testResult = testResult && temp;
		}

		if (testResult) {
			ContextComparator.addResult(namespace,
					ContextComparatorConfigReferences.TEST_CATEGORY_DATA,
					ContextComparatorConfigReferences.TEST_STATUS_PASSED,
					null);
		}
	}

	/**
	 * Compares count of rows as sanity.
	 * @param size1 Row count of context data 1.
	 * @param size2 Row count of context data 2.
	 */
	private final void checkRowCount(final int size1, final int size2) {
		if (size1 == size2) {
			ContextComparator.addResult(namespace,
					ContextComparatorConfigReferences.TEST_CATEGORY_ROW_COUNT,
					ContextComparatorConfigReferences.TEST_STATUS_PASSED,
					null);
			return;
		} else {
			LOGGER.error("Count of rows doesn't match.");
			ContextComparator.addResult(namespace,
					ContextComparatorConfigReferences.TEST_CATEGORY_ROW_COUNT,
					ContextComparatorConfigReferences.TEST_STATUS_FAILED,
					ContextComparator.CONTEXT_1.getName() + " : " + size1 + ", "
							+ ContextComparator.CONTEXT_2.getName() + " : " + size2);
			return;
		}
	}

	/**
	 * Compares key sets.
	 * @param set1 Key set of first context.
	 * @param set2 Key set of second context.
	 * @return Set of common keys.
	 */
	private final Set<String> checkKeySet(final Set<String> set1, final Set<String> set2) {
		if (set1.equals(set2)) {
			ContextComparator.addResult(namespace, 
					ContextComparatorConfigReferences.TEST_CATEGORY_KEY_SET,
					ContextComparatorConfigReferences.TEST_STATUS_PASSED,
					null);
			return set1;
		}

		LOGGER.error("Key sets don't match.");

		final Set<String> set1Copy = new HashSet<>();
		final Set<String> set2Copy = new HashSet<>();
		final Set<String> set1Backup = new HashSet<>();

		set1Copy.addAll(set1);
		set1Backup.addAll(set1);
		set2Copy.addAll(set2);

		set1Copy.removeAll(set2Copy);
		set2Copy.removeAll(set1Backup);

		ContextComparator.addResult(namespace,
				ContextComparatorConfigReferences.TEST_CATEGORY_KEY_SET,
				ContextComparatorConfigReferences.TEST_STATUS_FAILED,
				"Extra keys in " + ContextComparator.CONTEXT_1.getName()
				+ ": " + set1Copy + ". Extra keys in "
				+ ContextComparator.CONTEXT_2.getName() + ": "
				+ set2Copy);
		set1Backup.retainAll(set2);
		return set1Backup;
	}

	/**
	 * Compares a single row.
	 * @param row1 Row data from first context.
	 * @param row2 Row data from second context.
	 * @param rowKey Row key corresponding to the row data.
	 * @return Whether the comparison passed or not.
	 */
	private boolean checkRow(Map<String,String> row1, Map<String,String> row2, String rowKey) {
		final BasicDBList countColumns = (BasicDBList) config.get(
				ContextComparatorMongoReferences.FIELD_COUNT_COLUMNS);
		final BasicDBList refColumns = (BasicDBList) config.get(
				ContextComparatorMongoReferences.FIELD_REF_COLUMNS);

		// Compare count columns.
		int size = countColumns.size();
		boolean testResult = true;
		for (int i = 0; i < size; i ++) {
			final String colName = countColumns.get(i).toString();
			boolean flag = true;

			// Context 1 doesn't contain the column.
			if (!row1.containsKey(colName)) {
				ContextComparator.addResult(namespace,
						ContextComparatorConfigReferences.TEST_CATEGORY_COLUMN_LIST,
						ContextComparatorConfigReferences.TEST_STATUS_FAILED,
						colName + " column missing in " + ContextComparator.CONTEXT_1.getName()
						+ "for key: " + rowKey);
				flag = false;
				testResult = false;
			}

			// Context 2 doesn't contain the column.
			if (!row2.containsKey(colName)) {
				ContextComparator.addResult(namespace,
						ContextComparatorConfigReferences.TEST_CATEGORY_COLUMN_LIST,
						ContextComparatorConfigReferences.TEST_STATUS_FAILED,
						colName + " column missing in " + ContextComparator.CONTEXT_2.getName()
						+ "for key: " + rowKey);
				flag = false;
				testResult = false;
			}

			// Compare only if both contain the column.
			if (flag) {
				final boolean temp = compareIntValues(row1.get(colName), row2.get(colName),
						colName, rowKey);
				testResult = testResult && temp;
			}
		}

		// Compare other columns.
		size = refColumns.size();
		for (int i = 0; i < size; i ++) {
			final String colName = countColumns.get(i).toString();
			boolean flag = true;

			// Context 1 doesn't contain the column.
			if (!row1.containsKey(colName)) {
				ContextComparator.addResult(namespace,
						ContextComparatorConfigReferences.TEST_CATEGORY_COLUMN_LIST,
						ContextComparatorConfigReferences.TEST_STATUS_FAILED,
						colName + " column missing in " + ContextComparator.CONTEXT_1.getName()
						+ "for key: " + rowKey);
				flag = false;
				testResult = false;
			}

			// Context 2 doesn't contain the column.
			if (!row2.containsKey(colName)) {
				ContextComparator.addResult(namespace,
						ContextComparatorConfigReferences.TEST_CATEGORY_COLUMN_LIST,
						ContextComparatorConfigReferences.TEST_STATUS_FAILED,
						colName + " column missing in " + ContextComparator.CONTEXT_2.getName()
						+ "for key: " + rowKey);
				flag = false;
				testResult = false;
			}

			// Compare only if both contain the column.
			if (flag) {
				final boolean temp = compareStringValues(row1.get(colName),
						row2.get(colName), colName, rowKey);
				testResult = testResult && temp;
			}
		}

		// Return overall result.
		return testResult;
	}


	/**
	 * Compares context 1 and context 2 values with tolerance percentage.
	 * @param data1 Value from the CONTEXT1
	 * @param data2 Value from the CONTEXT2
	 * @param colName ColumnName from the CONTEXT1
	 * @param key Value contains RowKey
	 * @return Whether comparison passed or not.
	 */
	private final boolean compareIntValues(final String data1, final String data2,
			final String colName, final String key){
		int tolerance = ContextComparatorConfigReferences.DEFAULT_TOLERANCE_PERCENTAGE;
		try {
			tolerance = Integer.parseInt(
					config.get("tolerancePercent").toString());
		} catch (NumberFormatException e) {
			LOGGER.warn("Unable to parse tolerance percentage, falling back to default.", e);
		}

		// FIXME: What if values are decimals?
		int count1 = 0;
		int count2 = 0;
		try {
			count1 = Integer.parseInt(data1);
			count2 = Integer.parseInt(data2);
		} catch (NumberFormatException e) {
			LOGGER.error("Unable to parse count in " + colName + " for key: " + key, e);
			ContextComparator.addResult(namespace,
					ContextComparatorConfigReferences.TEST_CATEGORY_COUNT_VALUES,
					ContextComparatorConfigReferences.TEST_STATUS_FAILED,
					"Parse exception for " + colName + " and key " + key);
			return false;
		}

		if (count1 == count2) {
			return true;
		}

		final int error = Math.abs(count1 - count2)/count1*100;
		if (error < tolerance) {
			return true;
		}

		ContextComparator.addResult(namespace,
				ContextComparatorConfigReferences.TEST_CATEGORY_COUNT_VALUES,
				ContextComparatorConfigReferences.TEST_STATUS_FAILED,
				error + "% error for " + colName + " and key " + key
				+ ". " + ContextComparator.CONTEXT_1.getName()
				+ ": " + count1 + ", " + ContextComparator.CONTEXT_2.getName()
				+ ": " + count2);
		return false;
	}

	/**
	 * Compares data from context 1 and context 2 for exact match.
	 * @param data1 Value from the first context.
	 * @param data2 Value from the second context.
	 * @param colName Name of the column.
	 * @param key Row key for which data has been retrieved.
	 */
	private final boolean compareStringValues(final String data1,
			final String data2, final String colName, final String key) {
		if (data1.equals(data2)) {
			return true;
		}

		ContextComparator.addResult(namespace,
				ContextComparatorConfigReferences.TEST_CATEGORY_COLUMN_VALUES,
				ContextComparatorConfigReferences.TEST_STATUS_FAILED,
				"Mismatch for " + colName + " for key " + key
				+ ". " + ContextComparator.CONTEXT_1.getName() + ": " + data1
				+ ", " + ContextComparator.CONTEXT_2.getName() + ": " + data2);
		return false;
	}

}
