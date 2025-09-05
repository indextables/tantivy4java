package com.tantivy4java;

/**
 * A range query for split searching that matches documents with field values within a specified range.
 * Equivalent to Tantivy's RangeQuery but designed for QueryAst conversion.
 */
public class SplitRangeQuery extends SplitQuery {
    
    /**
     * Represents a range bound (inclusive, exclusive, or unbounded).
     */
    public static class RangeBound {
        private final String value;
        private final BoundType type;
        
        public enum BoundType {
            INCLUSIVE, EXCLUSIVE, UNBOUNDED
        }
        
        private RangeBound(String value, BoundType type) {
            this.value = value;
            this.type = type;
        }
        
        /**
         * Create an inclusive bound (value is included in the range).
         */
        public static RangeBound inclusive(String value) {
            if (value == null) {
                throw new IllegalArgumentException("Value cannot be null for inclusive bound");
            }
            return new RangeBound(value, BoundType.INCLUSIVE);
        }
        
        /**
         * Create an exclusive bound (value is excluded from the range).
         */
        public static RangeBound exclusive(String value) {
            if (value == null) {
                throw new IllegalArgumentException("Value cannot be null for exclusive bound");
            }
            return new RangeBound(value, BoundType.EXCLUSIVE);
        }
        
        /**
         * Create an unbounded range (no limit).
         */
        public static RangeBound unbounded() {
            return new RangeBound(null, BoundType.UNBOUNDED);
        }
        
        public String getValue() {
            return value;
        }
        
        public BoundType getType() {
            return type;
        }
        
        public boolean isUnbounded() {
            return type == BoundType.UNBOUNDED;
        }
        
        @Override
        public String toString() {
            switch (type) {
                case INCLUSIVE: return "[" + value;
                case EXCLUSIVE: return "(" + value;
                case UNBOUNDED: return "*";
                default: return "unknown";
            }
        }
    }
    
    private final String field;
    private final RangeBound lowerBound;
    private final RangeBound upperBound;
    private final String fieldType; // "i64", "u64", "f64", "str", "date", etc.
    
    /**
     * Create a new range query.
     * 
     * @param field The field name to search in
     * @param lowerBound The lower bound of the range
     * @param upperBound The upper bound of the range
     * @param fieldType The field type for proper value conversion ("i64", "u64", "f64", "str", "date")
     */
    public SplitRangeQuery(String field, RangeBound lowerBound, RangeBound upperBound, String fieldType) {
        if (field == null) {
            throw new IllegalArgumentException("Field cannot be null");
        }
        if (lowerBound == null || upperBound == null) {
            throw new IllegalArgumentException("Range bounds cannot be null");
        }
        this.field = field;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.fieldType = fieldType != null ? fieldType : "str";
    }
    
    /**
     * Create a range query with string field type (default).
     */
    public SplitRangeQuery(String field, RangeBound lowerBound, RangeBound upperBound) {
        this(field, lowerBound, upperBound, "str");
    }
    
    /**
     * Convenience method to create an inclusive range [lower TO upper].
     */
    public static SplitRangeQuery inclusiveRange(String field, String lower, String upper, String fieldType) {
        return new SplitRangeQuery(field, 
            lower != null ? RangeBound.inclusive(lower) : RangeBound.unbounded(),
            upper != null ? RangeBound.inclusive(upper) : RangeBound.unbounded(),
            fieldType);
    }
    
    /**
     * Convenience method to create an exclusive range (lower TO upper).
     */
    public static SplitRangeQuery exclusiveRange(String field, String lower, String upper, String fieldType) {
        return new SplitRangeQuery(field,
            lower != null ? RangeBound.exclusive(lower) : RangeBound.unbounded(),
            upper != null ? RangeBound.exclusive(upper) : RangeBound.unbounded(),
            fieldType);
    }
    
    public String getField() {
        return field;
    }
    
    public RangeBound getLowerBound() {
        return lowerBound;
    }
    
    public RangeBound getUpperBound() {
        return upperBound;
    }
    
    public String getFieldType() {
        return fieldType;
    }
    
    @Override
    public native String toQueryAstJson();
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("SplitRangeQuery(");
        sb.append("field=").append(field);
        sb.append(", range=");
        
        // Format as [lower TO upper] or similar
        if (lowerBound.isUnbounded()) {
            sb.append("*");
        } else {
            sb.append(lowerBound.getType() == RangeBound.BoundType.INCLUSIVE ? "[" : "(");
            sb.append(lowerBound.getValue());
        }
        
        sb.append(" TO ");
        
        if (upperBound.isUnbounded()) {
            sb.append("*");
        } else {
            sb.append(upperBound.getValue());
            sb.append(upperBound.getType() == RangeBound.BoundType.INCLUSIVE ? "]" : ")");
        }
        
        sb.append(", type=").append(fieldType);
        sb.append(")");
        return sb.toString();
    }
}