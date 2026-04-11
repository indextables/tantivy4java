# Memory Constants

Always use `Index.Memory` constants for heap allocation. Never hardcode byte values.

| Constant | Size | Use when |
|----------|------|----------|
| `Memory.MIN_HEAP_SIZE` | 15MB | Minimum required by Tantivy for any operation |
| `Memory.DEFAULT_HEAP_SIZE` | 50MB | Standard usage |
| `Memory.LARGE_HEAP_SIZE` | 128MB | Bulk operations, heavy indexing |
| `Memory.XL_HEAP_SIZE` | 256MB | Very large indices, high-performance scenarios |

Hardcoded heap sizes caused "memory arena needs to be at least 15000000" errors across 39+ test files. The constants were introduced to eliminate this entire class of bugs.
