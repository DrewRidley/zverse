//! UTF-8 order-preserving encoding module
//! 
//! This module provides functions to encode arbitrary UTF-8 strings into
//! order-preserving bitstreams that maintain exact lexicographic ordering.

use std::cmp::Ordering;

/// A bitstream that preserves lexicographic ordering of UTF-8 strings
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BitStream {
    bytes: Vec<u8>,
    bit_length: usize,
}

impl BitStream {
    /// Create a new empty bitstream
    pub fn new() -> Self {
        Self {
            bytes: Vec::new(),
            bit_length: 0,
        }
    }

    /// Create a bitstream with pre-allocated capacity
    pub fn with_capacity(capacity_bits: usize) -> Self {
        let byte_capacity = (capacity_bits + 7) / 8;
        Self {
            bytes: Vec::with_capacity(byte_capacity),
            bit_length: 0,
        }
    }

    /// Push a complete byte (8 bits) to the bitstream
    pub fn push_byte(&mut self, byte: u8) {
        self.bytes.push(byte);
        self.bit_length += 8;
    }

    /// Push individual bits to the bitstream
    pub fn push_bit(&mut self, bit: bool) {
        let byte_index = self.bit_length / 8;
        let bit_index = self.bit_length % 8;

        // Expand bytes vector if needed
        if byte_index >= self.bytes.len() {
            self.bytes.push(0);
        }

        // Set bit (MSB first within each byte)
        if bit {
            self.bytes[byte_index] |= 1 << (7 - bit_index);
        }

        self.bit_length += 1;
    }

    /// Get the number of bits in the stream
    pub fn bit_len(&self) -> usize {
        self.bit_length
    }

    /// Get the number of bytes used (rounded up)
    pub fn byte_len(&self) -> usize {
        (self.bit_length + 7) / 8
    }

    /// Get a bit at the specified index
    pub fn get_bit(&self, index: usize) -> Option<bool> {
        if index >= self.bit_length {
            return None;
        }

        let byte_index = index / 8;
        let bit_index = index % 8;
        let byte = self.bytes[byte_index];
        let bit = (byte >> (7 - bit_index)) & 1;
        Some(bit == 1)
    }

    /// Get the underlying bytes (may include padding bits in the last byte)
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Take up to `max_bits` from the start of the bitstream
    pub fn take(&self, max_bits: usize) -> BitStream {
        if max_bits >= self.bit_length {
            return self.clone();
        }

        let mut result = BitStream::with_capacity(max_bits);
        for i in 0..max_bits {
            if let Some(bit) = self.get_bit(i) {
                result.push_bit(bit);
            }
        }
        result
    }

    /// Convert to a vector of bits (for debugging/testing)
    pub fn to_bits(&self) -> Vec<bool> {
        (0..self.bit_length)
            .map(|i| self.get_bit(i).unwrap())
            .collect()
    }
}

impl PartialOrd for BitStream {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BitStream {
    fn cmp(&self, other: &Self) -> Ordering {
        // Compare bit by bit, treating shorter stream as if padded with zeros
        let max_len = self.bit_length.max(other.bit_length);
        
        for i in 0..max_len {
            let self_bit = self.get_bit(i).unwrap_or(false);
            let other_bit = other.get_bit(i).unwrap_or(false);
            
            match self_bit.cmp(&other_bit) {
                Ordering::Equal => continue,
                other => return other,
            }
        }
        
        Ordering::Equal
    }
}

/// Encode a UTF-8 string into an order-preserving bitstream
/// 
/// The encoding preserves lexicographic order exactly:
/// if s1 < s2 lexicographically, then encode(s1) < encode(s2)
/// 
/// The encoding is prefix-free due to the null terminator, so it can be
/// safely truncated at any bit boundary without ambiguity.
pub fn encode(key: &str) -> BitStream {
    let mut output = BitStream::with_capacity((key.len() + 1) * 8);
    
    // Encode each byte of the UTF-8 string
    for byte in key.bytes() {
        output.push_byte(byte);
    }
    
    // Add null terminator for prefix-free property
    output.push_byte(0x00);
    
    output
}

/// Encode a string and limit the output to a maximum number of bits
/// 
/// This is useful for Morton-T encoding where we only want the first
/// N bits of the key for interleaving with timestamp bits.
pub fn encode_limited(key: &str, max_bits: usize) -> BitStream {
    let full_encoding = encode(key);
    full_encoding.take(max_bits)
}

/// Decode a bitstream back to a UTF-8 string (for testing/debugging)
/// 
/// Returns None if the bitstream is not a valid encoding or doesn't
/// contain a null terminator.
pub fn decode(bitstream: &BitStream) -> Option<String> {
    if bitstream.bit_length % 8 != 0 {
        return None; // Must be byte-aligned
    }

    let bytes = bitstream.as_bytes();
    
    // Find null terminator
    let null_pos = bytes.iter().position(|&b| b == 0x00)?;
    
    // Extract string bytes (excluding null terminator)
    let string_bytes = &bytes[..null_pos];
    
    // Convert to UTF-8 string
    String::from_utf8(string_bytes.to_vec()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitstream_basic_operations() {
        let mut stream = BitStream::new();
        
        // Test empty stream
        assert_eq!(stream.bit_len(), 0);
        assert_eq!(stream.byte_len(), 0);
        
        // Test push_byte
        stream.push_byte(0b10110001);
        assert_eq!(stream.bit_len(), 8);
        assert_eq!(stream.byte_len(), 1);
        assert_eq!(stream.as_bytes(), &[0b10110001]);
        
        // Test get_bit
        assert_eq!(stream.get_bit(0), Some(true));  // MSB
        assert_eq!(stream.get_bit(1), Some(false));
        assert_eq!(stream.get_bit(2), Some(true));
        assert_eq!(stream.get_bit(7), Some(true));  // LSB
        assert_eq!(stream.get_bit(8), None);        // Out of bounds
    }

    #[test]
    fn test_bitstream_push_bit() {
        let mut stream = BitStream::new();
        
        // Push individual bits: 1011 0001
        stream.push_bit(true);
        stream.push_bit(false);
        stream.push_bit(true);
        stream.push_bit(true);
        stream.push_bit(false);
        stream.push_bit(false);
        stream.push_bit(false);
        stream.push_bit(true);
        
        assert_eq!(stream.bit_len(), 8);
        assert_eq!(stream.as_bytes(), &[0b10110001]);
    }

    #[test]
    fn test_bitstream_ordering() {
        let stream1 = {
            let mut s = BitStream::new();
            s.push_byte(0x01);
            s.push_byte(0x02);
            s
        };
        
        let stream2 = {
            let mut s = BitStream::new();
            s.push_byte(0x01);
            s.push_byte(0x03);
            s
        };
        
        let stream3 = {
            let mut s = BitStream::new();
            s.push_byte(0x01);
            s
        };
        
        assert!(stream1 < stream2);
        assert!(stream3 < stream1);
        assert!(stream3 < stream2);
        
        // Test equality
        let stream1_copy = {
            let mut s = BitStream::new();
            s.push_byte(0x01);
            s.push_byte(0x02);
            s
        };
        assert_eq!(stream1, stream1_copy);
    }

    #[test]
    fn test_encode_basic() {
        let encoded = encode("hello");
        
        // Should be: h(0x68) e(0x65) l(0x6C) l(0x6C) o(0x6F) null(0x00)
        let expected_bytes = vec![0x68, 0x65, 0x6C, 0x6C, 0x6F, 0x00];
        assert_eq!(encoded.as_bytes(), &expected_bytes);
        assert_eq!(encoded.bit_len(), 48); // 6 bytes * 8 bits
    }

    #[test]
    fn test_encode_empty_string() {
        let encoded = encode("");
        
        // Should be just the null terminator
        assert_eq!(encoded.as_bytes(), &[0x00]);
        assert_eq!(encoded.bit_len(), 8);
    }

    #[test]
    fn test_encode_order_preservation() {
        let test_cases = vec![
            "",
            "a",
            "aa",
            "ab",
            "b",
            "hello",
            "world",
            "user:123",
            "user:124",
            "user:alice",
            "user:bob",
        ];

        // Test that encoded order matches string order
        for i in 0..test_cases.len() {
            for j in 0..test_cases.len() {
                let s1 = test_cases[i];
                let s2 = test_cases[j];
                let e1 = encode(s1);
                let e2 = encode(s2);
                
                let string_cmp = s1.cmp(s2);
                let encoded_cmp = e1.cmp(&e2);
                
                assert_eq!(
                    string_cmp, encoded_cmp,
                    "Order mismatch: '{}' vs '{}' -> {:?} vs {:?}",
                    s1, s2, string_cmp, encoded_cmp
                );
            }
        }
    }

    #[test]
    fn test_encode_unicode() {
        let test_cases = vec![
            "café",
            "naïve", 
            "😀",
            "🚀",
            "Ω",
            "α",
            "β",
        ];

        // Test UTF-8 encoding preserves order
        for i in 0..test_cases.len() {
            for j in i+1..test_cases.len() {
                let s1 = test_cases[i];
                let s2 = test_cases[j];
                
                if s1 < s2 {
                    let e1 = encode(s1);
                    let e2 = encode(s2);
                    assert!(e1 < e2, "Unicode order not preserved: '{}' vs '{}'", s1, s2);
                }
            }
        }
    }

    #[test]
    fn test_encode_limited() {
        let key = "hello world";
        let limited = encode_limited(key, 32); // 4 bytes = 32 bits
        
        assert_eq!(limited.bit_len(), 32);
        
        // Should be first 4 bytes: "hell"
        assert_eq!(limited.as_bytes(), &[0x68, 0x65, 0x6C, 0x6C]);
    }

    #[test]
    fn test_encode_limited_preserves_order() {
        let keys = vec!["user:alice", "user:bob", "user:charlie"];
        let limited_encodings: Vec<_> = keys.iter()
            .map(|&k| encode_limited(k, 80))
            .collect();
        
        // Limited encodings should preserve order for keys with same prefix
        assert!(limited_encodings[0] < limited_encodings[1]);
        assert!(limited_encodings[1] < limited_encodings[2]);
    }

    #[test]
    fn test_decode() {
        let test_cases = vec![
            "",
            "hello",
            "world",
            "user:123",
            "café",
            "😀🚀",
        ];

        for &original in &test_cases {
            let encoded = encode(original);
            let decoded = decode(&encoded).expect("Failed to decode");
            assert_eq!(original, decoded);
        }
    }

    #[test]
    fn test_bitstream_take() {
        let stream = encode("hello");
        let taken = stream.take(24); // 3 bytes
        
        assert_eq!(taken.bit_len(), 24);
        assert_eq!(taken.as_bytes(), &[0x68, 0x65, 0x6C]); // "hel"
    }

    #[test]
    fn test_prefix_free_property() {
        // Test that prefixes don't interfere with ordering
        let short = encode("user");
        let long = encode("user:123");
        
        // "user" < "user:123" should be preserved
        assert!(short < long);
        
        // Test with common prefixes
        let cases = vec![
            ("a", "aa"),
            ("user", "user:1"),
            ("test", "testing"),
        ];
        
        for (shorter, longer) in cases {
            let short_enc = encode(shorter);
            let long_enc = encode(longer);
            assert!(short_enc < long_enc, "'{}' should sort before '{}'", shorter, longer);
        }
    }
}