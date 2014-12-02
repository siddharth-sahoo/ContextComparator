package com.awesome.pro.context.comparator;

import org.apache.log4j.Logger;

import com.awesome.pro.context.ContextBuilder;
import com.awesome.pro.context.ContextFactory;
import com.awesome.pro.context.comparator.references.ContextComparatorConfigReferences;
import com.awesome.pro.executor.IThreadPool;
import com.awesome.pro.executor.ThreadPool;
import com.awesome.pro.report.TestReport;
import com.awesome.pro.utilities.PropertyFileUtility;
import com.awesome.pro.utilities.db.mongo.MongoConnection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;

/**
 * This is the main class for context comparison making
 * it available in a static context.
 * @author balakrishnan.m
 */
public class ContextComparator {

	/**
	 * Root logger instance.
	 */
	private static final Logger LOGGER = Logger.getLogger(
			ContextComparator.class);

	/**
	 * Thread pool executor instance.
	 */
	private static IThreadPool EXECUTOR = null;

	/**
	 * Test report to be published.
	 */
	private static TestReport REPORT = null;

	/**
	 * First context builder instance to be compared.
	 */
	static ContextBuilder CONTEXT_1 = null;

	/**
	 * Second context builder instance to be compared.
	 */
	static ContextBuilder CONTEXT_2 = null;

	/**
	 * Cached configurations for comparison.
	 */
	private static PropertyFileUtility CONFIG = null;

	/**
	 * Initializes the required configurations and entities.
	 * @param configFile contains name of the file
	 */
	private static synchronized final void init(final String configFile) {
		if (CONFIG != null) {
			LOGGER.warn("Context comparison already in progress,"
					+ " ignoring new request.");
			return;
		}
		CONFIG = new PropertyFileUtility(configFile);

		// Get context reference.
		CONTEXT_1 = ContextFactory.getContextBuilder(
				CONFIG.getStringValue(
						ContextComparatorConfigReferences.PROPERTY_CONTEXT1
						)
				);
		CONTEXT_2 = ContextFactory.getContextBuilder(
				CONFIG.getStringValue(
						ContextComparatorConfigReferences.PROPERTY_CONTEXT2
						)
				);

		if (CONTEXT_1 == null) {
			LOGGER.error("Context not found: "
					+ CONFIG.getStringValue(
							ContextComparatorConfigReferences.PROPERTY_CONTEXT1
							));
			stop();
			System.exit(1);
		}

		if (CONTEXT_2 == null) {
			LOGGER.error("Context not found: "
					+ CONFIG.getStringValue(
							ContextComparatorConfigReferences.PROPERTY_CONTEXT2
							));
			stop();
			System.exit(1);
		}

		// Initialize test report.
		REPORT = TestReport.getTestReport(
				CONFIG.getStringValue(
						ContextComparatorConfigReferences.PROPERTY_REPORT_NAME
						)
				);
		REPORT.addTable(
				REPORT.getTableBuilder(ContextComparatorConfigReferences.
						DEFAULT_REPORT_TABLE_NAME)
						.setSubTitle(CONFIG.getStringValue(
								ContextComparatorConfigReferences.
								PROPERTY_REPORT_SUB_TITLE)
								)
								.enableCategorization(true)
								.enableComments(true)
								.enableHeader(true)
								.enableStatus(true)
								.build());

		// Initialize thread pool executor.
		EXECUTOR = new ThreadPool(configFile);
		EXECUTOR.start();
	}

	/**
	 * Starts context comparison according to the input the configuration file.
	 * @param configFile Name of the configuration file to be read.
	 */
	public static final void start(final String configFile) {
		init(configFile);
		final DBCursor cursor =  MongoConnection.getDocuments(
				CONFIG.getStringValue(ContextComparatorConfigReferences.
						PROPERTY_MONGO_DATABASE_NAME),
						CONFIG.getStringValue(ContextComparatorConfigReferences.
								PROPERTY_MONGO_DATABASE_COLLECTION)
				);

		while (cursor.hasNext()) {
			final BasicDBObject obj = (BasicDBObject) cursor.next();
			EXECUTOR.execute(new ContextComparatorRunnableJob(obj));
		}

		LOGGER.info("Waiting for completion.");
		EXECUTOR.waitForCompletion();
		REPORT.publish(CONFIG.getStringValue(
				ContextComparatorConfigReferences.PROPERTY_REPORT_FILE_NAME,
				ContextComparatorConfigReferences.DEFAULT_REPORT_FILE_NAME
				+ System.currentTimeMillis()
				+ ContextComparatorConfigReferences.DEFAULT_REPORT_FILE_EXTENSION
				));

		stop();
	}

	/**
	 * Tears down all initializations.
	 */
	private static final void stop() {
		EXECUTOR.shutdown();
		EXECUTOR = null;
		CONFIG = null;
		CONTEXT_1 = null;
		CONTEXT_2 = null;
		REPORT = null;
		LOGGER.info("Cleaning up complete.");
	}

	/**
	 * Adds a result to the test report.
	 * @param category Name space in the context under test.
	 * @param testCase Test being performed on the name space.
	 * @param status Status of the test - passed/failed/skipped.
	 * @param comments Description of the reason.
	 */
	public static synchronized final void addResult(final String category,
			final String testCase, final String status, final String comments) {
		REPORT.getTable(ContextComparatorConfigReferences.
				DEFAULT_REPORT_TABLE_NAME)
				.addResult(category, testCase, status, comments);
	}

}

