package com.tantivy4java;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;

import java.io.IOException;

/**
 * Performance test comparing classic individual document retrieval vs zero-copy bulk retrieval
 * with 100,000 documents. This test provides a baseline for medium-scale document retrieval.
 */
public class HundredKRecordBulkRetrievalTest extends BaseBulkRetrievalPerformanceTest {

    private static final int TOTAL_DOCUMENTS = 100_000;

    @Override
    protected int getTotalDocuments() {
        return TOTAL_DOCUMENTS;
    }

    @Override
    protected String getTestName() {
        return "HundredK-Record";
    }

    @BeforeAll
    static void setUpOnce() throws IOException {
        HundredKRecordBulkRetrievalTest instance = new HundredKRecordBulkRetrievalTest();
        instance.performSetup();
    }
}