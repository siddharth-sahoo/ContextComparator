package com.awesome.pro.context.comparator.references;

/**
 * Configuration related reference constants related to
 * context comparison.
 * @author balakrishnan.m
 */
public class ContextComparatorConfigReferences {

	// Configuration Parameters.
	public static final String PROPERTY_MONGO_DATABASE_NAME = "MongoDatabase";
	public static final String PROPERTY_MONGO_DATABASE_COLLECTION = "MongoCollection";

	public static final String PROPERTY_CONTEXT1 = "Context1";
	public static final String PROPERTY_CONTEXT2 = "Context2";

	public static final String PROPERTY_REPORT_NAME = "ReportTitle";
	public static final String PROPERTY_REPORT_SUB_TITLE = "ReportSubTitle";
	public static final String PROPERTY_REPORT_FILE_NAME = "ReportFile";

	// Default Configurations.
	public static final String DEFAULT_REPORT_TABLE_NAME = "Table1";
	public static final String DEFAULT_REPORT_FILE_NAME = "ContextComparison_";
	public static final String DEFAULT_REPORT_FILE_EXTENSION = ".html";

	// Test Report References.
	public static final String TEST_STATUS_PASSED = "Passed";
	public static final String TEST_STATUS_FAILED = "Failed";
	public static final String TEST_STATUS_SKIPPED = "Skipped";

	// Test categories.
	public static final String TEST_CATEGORY_SANITY = "Sanity";
	public static final String TEST_CATEGORY_ROW_COUNT = "Count of rows";
	public static final String TEST_CATEGORY_KEY_SET = "Key set comparison";
	public static final String TEST_CATEGORY_COLUMN_LIST = "Column sanity";
	public static final String TEST_CATEGORY_COUNT_VALUES = "Count column data";
	public static final String TEST_CATEGORY_COLUMN_VALUES = "Other column data";
	public static final String TEST_CATEGORY_DATA =  "Data quality";

	// Comparison constants.
	public static final int DEFAULT_TOLERANCE_PERCENTAGE = 0;
	
}
