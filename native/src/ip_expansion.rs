// ip_expansion.rs - CIDR and wildcard IP pattern expansion
//
// Provides transparent AST rewriting so callers can pass CIDR (192.168.1.0/24)
// or wildcard (192.168.1.*) values in term queries and get correct range semantics.
//
// Performance design:
//   - try_expand_cidr:            pre-check value.contains('/') — zero allocation for regular IPs
//   - try_expand_ipv4_wildcard:   pre-check value.contains('*') — zero allocation for non-wildcards
//   - try_expand_ip_range:        calls both, first match wins
//   - #[inline] on all three:     branch eliminated in the hot no-match case

use std::net::{Ipv4Addr, Ipv6Addr};

/// Try to expand an IPv4 or IPv6 CIDR notation to (lower, upper) IP string bounds.
/// Returns None immediately (zero allocation) if the value doesn't contain '/'.
/// Handles IPv4 (/0–/32) and IPv6 (/0–/128).
#[inline]
pub fn try_expand_cidr(value: &str) -> Option<(String, String)> {
    // Fast path: if no '/', not a CIDR — regular IPs never contain '/'
    if !value.contains('/') {
        return None;
    }

    let slash_pos = value.rfind('/')?;
    let ip_part = &value[..slash_pos];
    let prefix_part = &value[slash_pos + 1..];

    let prefix_len: u32 = prefix_part.parse().ok()?;

    // Try IPv4 first
    if let Ok(ipv4) = ip_part.parse::<Ipv4Addr>() {
        if prefix_len > 32 {
            return None;
        }
        let ip_u32 = u32::from(ipv4);
        let mask: u32 = if prefix_len == 0 {
            0
        } else {
            !0u32 << (32 - prefix_len)
        };
        let lower = Ipv4Addr::from(ip_u32 & mask);
        let upper = Ipv4Addr::from((ip_u32 & mask) | !mask);
        return Some((lower.to_string(), upper.to_string()));
    }

    // Try IPv6
    if let Ok(ipv6) = ip_part.parse::<Ipv6Addr>() {
        if prefix_len > 128 {
            return None;
        }
        let ip_u128 = u128::from(ipv6);
        let mask: u128 = if prefix_len == 0 {
            0
        } else {
            !0u128 << (128 - prefix_len)
        };
        let lower = Ipv6Addr::from(ip_u128 & mask);
        let upper = Ipv6Addr::from((ip_u128 & mask) | !mask);
        return Some((lower.to_string(), upper.to_string()));
    }

    None
}

/// Try to expand an IPv4 wildcard pattern (e.g., "192.168.1.*") to (lower, upper) bounds.
/// Returns None immediately (zero allocation) if the value doesn't contain '*'.
/// IPv6 wildcards are not standard and are not supported.
/// Any octet position may be '*' (e.g., "10.*.1.*" is valid).
#[inline]
pub fn try_expand_ipv4_wildcard(value: &str) -> Option<(String, String)> {
    // Fast path: if no '*', not a wildcard — regular IPs never contain '*'
    if !value.contains('*') {
        return None;
    }

    let parts: Vec<&str> = value.split('.').collect();
    if parts.len() != 4 {
        return None;
    }

    // Reject non-contiguous wildcard patterns like "10.*.1.*" where a fixed octet
    // follows a wildcard octet.  A single range [10.0.1.0 TO 10.255.1.255] would
    // include addresses with wrong third octets (false positives).  Return None so
    // the caller treats the value as a literal term and the user gets an explicit error
    // rather than silently incorrect results.
    let mut seen_wildcard = false;
    for part in &parts {
        if *part == "*" {
            seen_wildcard = true;
        } else if seen_wildcard {
            // Fixed octet after a wildcard — non-contiguous, cannot represent as a single range.
            return None;
        }
    }

    let mut lower_parts = [0u8; 4];
    let mut upper_parts = [0u8; 4];

    for (i, part) in parts.iter().enumerate() {
        if *part == "*" {
            lower_parts[i] = 0;
            upper_parts[i] = 255;
        } else {
            let octet: u8 = part.parse().ok()?;
            lower_parts[i] = octet;
            upper_parts[i] = octet;
        }
    }

    let lower = format!(
        "{}.{}.{}.{}",
        lower_parts[0], lower_parts[1], lower_parts[2], lower_parts[3]
    );
    let upper = format!(
        "{}.{}.{}.{}",
        upper_parts[0], upper_parts[1], upper_parts[2], upper_parts[3]
    );

    Some((lower, upper))
}

/// Try CIDR first, then IPv4 wildcard. Returns first match or None.
/// Called on every IP term query — must be fast in the no-match case.
#[inline]
pub fn try_expand_ip_range(value: &str) -> Option<(String, String)> {
    try_expand_cidr(value).or_else(|| try_expand_ipv4_wildcard(value))
}

/// Check if the given (lower, upper) pair represents a full match-all range.
/// Handles both IPv4 (0.0.0.0 – 255.255.255.255) and IPv6 (:: – ffff:...) match-all cases.
/// Used to emit MatchAll/AllQuery for maximum performance instead of scanning the full IP space.
#[inline]
pub fn is_match_all_range(lower: &str, upper: &str) -> bool {
    // IPv4 match-all: from *.*.*.*  or  0.0.0.0/0
    if lower == "0.0.0.0" && upper == "255.255.255.255" {
        return true;
    }
    // IPv6 match-all: from ::/0
    // Guard on ':' avoids two Ipv6Addr parses for the common IPv4 case.
    if lower.contains(':') {
        if let (Ok(l), Ok(u)) = (lower.parse::<Ipv6Addr>(), upper.parse::<Ipv6Addr>()) {
            if u128::from(l) == 0 && u128::from(u) == u128::MAX {
                return true;
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cidr_ipv4_24() {
        let result = try_expand_cidr("192.168.1.0/24");
        assert_eq!(result, Some(("192.168.1.0".to_string(), "192.168.1.255".to_string())));
    }

    #[test]
    fn test_cidr_ipv4_16() {
        let result = try_expand_cidr("10.0.0.0/16");
        assert_eq!(result, Some(("10.0.0.0".to_string(), "10.0.255.255".to_string())));
    }

    #[test]
    fn test_cidr_ipv4_8() {
        let result = try_expand_cidr("10.0.0.0/8");
        assert_eq!(result, Some(("10.0.0.0".to_string(), "10.255.255.255".to_string())));
    }

    #[test]
    fn test_cidr_ipv4_32_exact() {
        let result = try_expand_cidr("192.168.1.1/32");
        assert_eq!(result, Some(("192.168.1.1".to_string(), "192.168.1.1".to_string())));
    }

    #[test]
    fn test_cidr_ipv4_0_match_all() {
        let result = try_expand_cidr("0.0.0.0/0");
        assert_eq!(result, Some(("0.0.0.0".to_string(), "255.255.255.255".to_string())));
        let (lower, upper) = result.unwrap();
        assert!(is_match_all_range(&lower, &upper));
    }

    #[test]
    fn test_cidr_ipv4_non_byte_aligned_25() {
        let result = try_expand_cidr("192.168.1.0/25");
        assert_eq!(result, Some(("192.168.1.0".to_string(), "192.168.1.127".to_string())));
    }

    #[test]
    fn test_cidr_ipv4_non_byte_aligned_17() {
        let result = try_expand_cidr("172.16.0.0/17");
        assert_eq!(result, Some(("172.16.0.0".to_string(), "172.16.127.255".to_string())));
    }

    #[test]
    fn test_cidr_ipv6_32() {
        let result = try_expand_cidr("2001:db8::/32");
        assert!(result.is_some());
        let (lower, upper) = result.unwrap();
        assert_eq!(lower, "2001:db8::");
        // upper should have the lower 96 bits set to all-ones
        let upper_ip: Ipv6Addr = upper.parse().unwrap();
        let upper_u128 = u128::from(upper_ip);
        // top 32 bits: 2001:0db8 = 0x20010db8
        assert_eq!(upper_u128 >> 96, 0x20010db8u128);
        // lower 96 bits: all ones
        let lower_bits_mask = (1u128 << 96) - 1;
        assert_eq!(upper_u128 & lower_bits_mask, lower_bits_mask);
    }

    #[test]
    fn test_cidr_ipv6_128_exact() {
        let result = try_expand_cidr("::1/128");
        assert_eq!(result, Some(("::1".to_string(), "::1".to_string())));
    }

    #[test]
    fn test_cidr_ipv6_0_match_all() {
        let result = try_expand_cidr("::/0");
        assert!(result.is_some());
        let (lower, upper) = result.unwrap();
        assert!(is_match_all_range(&lower, &upper));
    }

    #[test]
    fn test_regular_ipv4_no_match() {
        assert_eq!(try_expand_cidr("192.168.1.1"), None);
        assert_eq!(try_expand_ipv4_wildcard("192.168.1.1"), None);
        assert_eq!(try_expand_ip_range("192.168.1.1"), None);
    }

    #[test]
    fn test_wildcard_last_octet() {
        let result = try_expand_ipv4_wildcard("192.168.1.*");
        assert_eq!(result, Some(("192.168.1.0".to_string(), "192.168.1.255".to_string())));
    }

    #[test]
    fn test_wildcard_two_octets() {
        let result = try_expand_ipv4_wildcard("10.0.*.*");
        assert_eq!(result, Some(("10.0.0.0".to_string(), "10.0.255.255".to_string())));
    }

    #[test]
    fn test_wildcard_three_octets() {
        let result = try_expand_ipv4_wildcard("10.*.*.*");
        assert_eq!(result, Some(("10.0.0.0".to_string(), "10.255.255.255".to_string())));
    }

    #[test]
    fn test_wildcard_mixed_positions() {
        // Non-contiguous wildcard: a fixed octet (1) follows a wildcard octet (*).
        // A single range would produce false positives (e.g. 10.5.2.100 would match).
        // We reject these patterns so callers get an explicit error instead of wrong results.
        let result = try_expand_ipv4_wildcard("10.*.1.*");
        assert_eq!(result, None);
    }

    #[test]
    fn test_wildcard_all_octets_match_all() {
        let result = try_expand_ipv4_wildcard("*.*.*.*");
        assert_eq!(result, Some(("0.0.0.0".to_string(), "255.255.255.255".to_string())));
        let (lower, upper) = result.unwrap();
        assert!(is_match_all_range(&lower, &upper));
    }

    #[test]
    fn test_wildcard_equivalence_to_cidr_24() {
        let cidr = try_expand_cidr("192.168.1.0/24");
        let wildcard = try_expand_ipv4_wildcard("192.168.1.*");
        assert_eq!(cidr, wildcard);
    }

    #[test]
    fn test_invalid_cidr_prefix() {
        assert_eq!(try_expand_cidr("192.168.1.0/33"), None);
        assert_eq!(try_expand_cidr("::1/129"), None);
    }

    #[test]
    fn test_invalid_wildcard_not_four_parts() {
        assert_eq!(try_expand_ipv4_wildcard("192.168.*"), None);
        assert_eq!(try_expand_ipv4_wildcard("*"), None);
    }

    #[test]
    fn test_is_match_all_range() {
        assert!(is_match_all_range("0.0.0.0", "255.255.255.255"));
        assert!(!is_match_all_range("10.0.0.0", "10.255.255.255"));
    }

    #[test]
    fn test_cidr_ipv6_non_byte_aligned_10() {
        // fe80::/10 is the link-local unicast range — 10-bit prefix, non-byte-aligned
        let result = try_expand_cidr("fe80::/10");
        assert!(result.is_some());
        let (lower, upper) = result.unwrap();

        let lower_ip: Ipv6Addr = lower.parse().unwrap();
        let upper_ip: Ipv6Addr = upper.parse().unwrap();
        let lower_u128 = u128::from(lower_ip);
        let upper_u128 = u128::from(upper_ip);

        // Top 10 bits of fe80:: are 0b1111_1110_10 (= 1018 decimal)
        assert_eq!(lower_u128 >> 118, 0b1111_1110_10u128);

        // Top 10 bits of upper must equal top 10 bits of lower
        assert_eq!(upper_u128 >> 118, lower_u128 >> 118);

        // Lower 118 bits of upper must all be 1
        let lower_bits_mask: u128 = (1u128 << 118) - 1;
        assert_eq!(upper_u128 & lower_bits_mask, lower_bits_mask);
    }
}
