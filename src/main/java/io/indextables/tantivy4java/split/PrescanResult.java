package io.indextables.tantivy4java.split;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Result of prescanning a split for potential query matches.
 *
 * <p>The prescan performs a <b>conservative approximation</b>:
 * <ul>
 *   <li><b>"No results" = CERTAIN</b> - term doesn't exist in FST, so no documents can match</li>
 *   <li><b>"Maybe results" = UNCERTAIN</b> - term exists, but full query might still return 0</li>
 * </ul>
 *
 * <p>This is the correct behavior for filtering - we eliminate splits that DEFINITELY won't match,
 * while conservatively including splits that might match.
 *
 * <p>Usage example:
 * <pre>{@code
 * List<PrescanResult> results = cacheManager.prescanSplits(splits, docMapping, query);
 *
 * // Filter to matching splits
 * List<String> matchingSplits = results.stream()
 *     .filter(PrescanResult::couldHaveResults)
 *     .map(PrescanResult::getSplitUrl)
 *     .collect(Collectors.toList());
 *
 * // Check why a split was excluded
 * for (PrescanResult result : results) {
 *     if (!result.couldHaveResults()) {
 *         System.out.println(result.getSplitUrl() + " excluded: " +
 *             result.getTermExistence());
 *     }
 * }
 * }</pre>
 */
public class PrescanResult {

    /**
     * Status of the prescan operation for this split.
     */
    public enum PrescanStatus {
        /**
         * Prescan completed normally.
         */
        SUCCESS,

        /**
         * Prescan timed out - split is conservatively included (couldHaveResults=true).
         */
        TIMEOUT,

        /**
         * Error occurred during prescan - split is conservatively included (couldHaveResults=true).
         */
        ERROR
    }

    private final String splitUrl;
    private final boolean couldHaveResults;
    private final Map<String, Boolean> termExistence;
    private final PrescanStatus status;
    private final String errorMessage;

    /**
     * Create a successful prescan result.
     *
     * @param splitUrl The split URL that was prescanned
     * @param couldHaveResults Whether the split could have results for the query
     * @param termExistence Map of "field:term" to existence boolean
     */
    public PrescanResult(String splitUrl, boolean couldHaveResults, Map<String, Boolean> termExistence) {
        this.splitUrl = Objects.requireNonNull(splitUrl, "splitUrl is required");
        this.couldHaveResults = couldHaveResults;
        this.termExistence = termExistence != null
            ? Collections.unmodifiableMap(new HashMap<>(termExistence))
            : Collections.emptyMap();
        this.status = PrescanStatus.SUCCESS;
        this.errorMessage = null;
    }

    /**
     * Create a prescan result with a specific status.
     *
     * @param splitUrl The split URL that was prescanned
     * @param couldHaveResults Whether the split could have results for the query
     * @param termExistence Map of "field:term" to existence boolean
     * @param status The status of the prescan operation
     * @param errorMessage Error message if status is ERROR or TIMEOUT
     */
    public PrescanResult(
            String splitUrl,
            boolean couldHaveResults,
            Map<String, Boolean> termExistence,
            PrescanStatus status,
            String errorMessage) {
        this.splitUrl = Objects.requireNonNull(splitUrl, "splitUrl is required");
        this.couldHaveResults = couldHaveResults;
        this.termExistence = termExistence != null
            ? Collections.unmodifiableMap(new HashMap<>(termExistence))
            : Collections.emptyMap();
        this.status = status != null ? status : PrescanStatus.SUCCESS;
        this.errorMessage = errorMessage;
    }

    /**
     * Create an error result that conservatively includes the split.
     *
     * @param splitUrl The split URL
     * @param error The error that occurred
     * @return A PrescanResult with couldHaveResults=true and ERROR status
     */
    public static PrescanResult error(String splitUrl, Throwable error) {
        return new PrescanResult(
            splitUrl,
            true,  // Conservative: include on error
            Collections.emptyMap(),
            PrescanStatus.ERROR,
            error != null ? error.getMessage() : "Unknown error"
        );
    }

    /**
     * Create an error result with a message.
     *
     * @param splitUrl The split URL
     * @param errorMessage The error message
     * @return A PrescanResult with couldHaveResults=true and ERROR status
     */
    public static PrescanResult error(String splitUrl, String errorMessage) {
        return new PrescanResult(
            splitUrl,
            true,  // Conservative: include on error
            Collections.emptyMap(),
            PrescanStatus.ERROR,
            errorMessage
        );
    }

    /**
     * Create a timeout result that conservatively includes the split.
     *
     * @param splitUrl The split URL
     * @return A PrescanResult with couldHaveResults=true and TIMEOUT status
     */
    public static PrescanResult timeout(String splitUrl) {
        return new PrescanResult(
            splitUrl,
            true,  // Conservative: include on timeout
            Collections.emptyMap(),
            PrescanStatus.TIMEOUT,
            "Prescan timed out"
        );
    }

    /**
     * Get the split URL that was prescanned.
     *
     * @return The split URL
     */
    public String getSplitUrl() {
        return splitUrl;
    }

    /**
     * Check if this split could have results for the query.
     *
     * <p>If true, the split should be included in the full search.
     * If false, the split can be safely skipped (no matching terms exist).
     *
     * @return true if the split could have results, false if it definitely won't
     */
    public boolean couldHaveResults() {
        return couldHaveResults;
    }

    /**
     * Get the term existence map.
     *
     * <p>Keys are in the format "field:term" and values indicate whether
     * the term exists in the split's FST (term dictionary).
     *
     * @return Unmodifiable map of term existence
     */
    public Map<String, Boolean> getTermExistence() {
        return termExistence;
    }

    /**
     * Get the prescan status.
     *
     * @return The status (SUCCESS, TIMEOUT, or ERROR)
     */
    public PrescanStatus getStatus() {
        return status;
    }

    /**
     * Get the error message if status is ERROR or TIMEOUT.
     *
     * @return The error message, or null if status is SUCCESS
     */
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * Check if the prescan was successful.
     *
     * @return true if status is SUCCESS
     */
    public boolean isSuccess() {
        return status == PrescanStatus.SUCCESS;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PrescanResult(splitUrl=").append(splitUrl);
        sb.append(", couldHaveResults=").append(couldHaveResults);
        sb.append(", status=").append(status);
        if (!termExistence.isEmpty()) {
            sb.append(", termExistence=").append(termExistence);
        }
        if (errorMessage != null) {
            sb.append(", errorMessage=").append(errorMessage);
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        PrescanResult that = (PrescanResult) obj;
        return couldHaveResults == that.couldHaveResults
            && splitUrl.equals(that.splitUrl)
            && termExistence.equals(that.termExistence)
            && status == that.status
            && Objects.equals(errorMessage, that.errorMessage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitUrl, couldHaveResults, termExistence, status, errorMessage);
    }
}
