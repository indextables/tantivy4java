package io.indextables.tantivy4java.split;

import java.util.Objects;

/**
 * Contains split URL, footer offset, and file size for efficient prescan.
 *
 * <p>All three fields are required to avoid S3/Azure HEAD requests during prescan:
 * <ul>
 *   <li>{@code splitUrl} - The split file location (s3://, azure://, file://)</li>
 *   <li>{@code footerOffset} - Where the footer starts (= split_footer_start from metastore)</li>
 *   <li>{@code fileSize} - Total file size (= split_footer_end from metastore)</li>
 * </ul>
 *
 * <p>These values should come from your metastore, which stores split metadata
 * including footer offsets when splits are created.
 *
 * <p>Usage example:
 * <pre>{@code
 * // Query your metastore for split metadata
 * // SELECT split_url, footer_start, footer_end FROM splits WHERE index_id = ?
 *
 * List<SplitInfo> splits = Arrays.asList(
 *     new SplitInfo("s3://bucket/split1.split", footerStart1, fileSize1),
 *     new SplitInfo("s3://bucket/split2.split", footerStart2, fileSize2)
 * );
 *
 * List<PrescanResult> results = cacheManager.prescanSplits(splits, docMapping, query);
 * }</pre>
 */
public class SplitInfo {
    private final String splitUrl;
    private final long footerOffset;
    private final long fileSize;

    /**
     * Create a new SplitInfo with all required fields.
     *
     * @param splitUrl The split URL (s3://, azure://, file://)
     * @param footerOffset The byte offset where the footer starts (split_footer_start)
     * @param fileSize The total file size in bytes (split_footer_end)
     * @throws NullPointerException if splitUrl is null
     * @throws IllegalArgumentException if footerOffset is negative, fileSize is invalid,
     *         or fileSize <= footerOffset
     */
    public SplitInfo(String splitUrl, long footerOffset, long fileSize) {
        this.splitUrl = Objects.requireNonNull(splitUrl, "splitUrl is required");
        if (footerOffset < 0) {
            throw new IllegalArgumentException("footerOffset must be >= 0, got: " + footerOffset);
        }
        if (fileSize <= 0) {
            throw new IllegalArgumentException("fileSize must be > 0, got: " + fileSize);
        }
        if (fileSize <= footerOffset) {
            throw new IllegalArgumentException("fileSize (" + fileSize + ") must be > footerOffset (" + footerOffset + ")");
        }
        this.footerOffset = footerOffset;
        this.fileSize = fileSize;
    }

    /**
     * Get the split URL.
     *
     * @return The split URL (s3://, azure://, or file://)
     */
    public String getSplitUrl() {
        return splitUrl;
    }

    /**
     * Get the footer offset (split_footer_start).
     *
     * @return The byte offset where the footer starts in the split file
     */
    public long getFooterOffset() {
        return footerOffset;
    }

    /**
     * Get the file size (split_footer_end).
     *
     * @return The total file size in bytes
     */
    public long getFileSize() {
        return fileSize;
    }

    @Override
    public String toString() {
        return String.format("SplitInfo(splitUrl=%s, footerOffset=%d, fileSize=%d)",
                           splitUrl, footerOffset, fileSize);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        SplitInfo that = (SplitInfo) obj;
        return footerOffset == that.footerOffset &&
               fileSize == that.fileSize &&
               splitUrl.equals(that.splitUrl);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitUrl, footerOffset, fileSize);
    }
}
