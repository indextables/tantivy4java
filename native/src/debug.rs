use once_cell::sync::Lazy;
use std::env;

/// Global debug flag, evaluated once at startup
pub static DEBUG_ENABLED: Lazy<bool> = Lazy::new(|| {
    env::var("TANTIVY4JAVA_DEBUG")
        .map(|v| v == "1" || v.to_lowercase() == "true")
        .unwrap_or(false)
});

/// Macro for conditional debug printing
#[macro_export]
macro_rules! debug_println {
    ($($arg:tt)*) => {
        if *crate::debug::DEBUG_ENABLED {
            eprintln!($($arg)*);
        }
    };
}