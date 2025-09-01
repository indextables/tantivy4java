package com.tantivy4java;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;

import java.io.IOException;

/**
 * Performance test comparing classic individual document retrieval vs zero-copy bulk retrieval
 * with 250,000 documents. This test validates performance for large-scale document retrieval.
 */
public class TwoFiftyKRecordBulkRetrievalTest extends BaseBulkRetrievalPerformanceTest {

    private static final int TOTAL_DOCUMENTS = 250_000;

    @Override
    protected int getTotalDocuments() {
        return TOTAL_DOCUMENTS;
    }

    @Override
    protected String getTestName() {
        return "TwoFiftyK-Record";
    }

    @BeforeAll
    static void setUpOnce() throws IOException {
        TwoFiftyKRecordBulkRetrievalTest instance = new TwoFiftyKRecordBulkRetrievalTest();
        instance.performSetup();
    }
}