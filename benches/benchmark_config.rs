use rand::prelude::SliceRandom;
use rand::SeedableRng;
use rand::{rngs::StdRng, Rng};

/// Configuration for benchmark test scenarios
#[derive(Clone, Debug)]
pub struct BenchmarkConfig {
    pub dataset_sizes: Vec<usize>,
    pub key_sizes: Vec<usize>,
    pub value_sizes: Vec<usize>,
    pub thread_counts: Vec<usize>,
    pub read_ratios: Vec<usize>,
    pub batch_sizes: Vec<usize>,
    pub versions_per_key: Vec<usize>,
    pub sample_size: usize,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            dataset_sizes: vec![10_000, 50_000, 100_000, 500_000],
            key_sizes: vec![16, 32, 64, 128, 256],
            value_sizes: vec![32, 128, 512, 1024, 4096],
            thread_counts: vec![1, 2, 4, 8, 16],
            read_ratios: vec![50, 70, 80, 90, 95],
            batch_sizes: vec![10, 100, 1000, 10000],
            versions_per_key: vec![5, 10, 20, 50],
            sample_size: 100,
        }
    }
}

/// Quick configuration for development testing
impl BenchmarkConfig {
    pub fn quick() -> Self {
        Self {
            dataset_sizes: vec![1_000, 10_000],
            key_sizes: vec![32, 128],
            value_sizes: vec![128, 1024],
            thread_counts: vec![1, 4],
            read_ratios: vec![80],
            batch_sizes: vec![100, 1000],
            versions_per_key: vec![5],
            sample_size: 10,
        }
    }

    pub fn stress() -> Self {
        Self {
            dataset_sizes: vec![1_000_000, 5_000_000, 10_000_000],
            key_sizes: vec![64, 256, 1024],
            value_sizes: vec![1024, 4096, 16384, 65536],
            thread_counts: vec![4, 8, 16, 32],
            read_ratios: vec![50, 90],
            batch_sizes: vec![10000, 100000],
            versions_per_key: vec![10, 50, 100],
            sample_size: 10,
        }
    }

    pub fn production() -> Self {
        Self {
            dataset_sizes: vec![100_000, 1_000_000, 10_000_000],
            key_sizes: vec![32, 64, 128, 256],
            value_sizes: vec![256, 1024, 4096, 16384],
            thread_counts: vec![1, 2, 4, 8, 16, 32],
            read_ratios: vec![70, 80, 90, 95, 99],
            batch_sizes: vec![100, 1000, 10000, 50000],
            versions_per_key: vec![5, 10, 20, 50, 100],
            sample_size: 100,
        }
    }
}

/// Data generation utilities
#[derive(Clone, Copy)]
pub struct DataGenerator {
    pub seed: u64,
}

impl DataGenerator {
    pub fn new(seed: u64) -> Self {
        Self { seed }
    }

    pub fn seeded_rng(&self, salt: u64) -> StdRng {
        StdRng::seed_from_u64(self.seed ^ salt)
    }

    /// Generate keys with specified size and patterns
    pub fn generate_keys(&self, count: usize, key_size: usize, pattern: KeyPattern) -> Vec<String> {
        let mut rng = self.seeded_rng(0x1000);
        let mut keys = Vec::with_capacity(count);

        match pattern {
            KeyPattern::Sequential => {
                for i in 0..count {
                    let key = self.format_key_with_size(&format!("seq_{:016x}", i), key_size, &mut rng);
                    keys.push(key);
                }
            }
            KeyPattern::Random => {
                for i in 0..count {
                    let random_part: u64 = rng.gen();
                    let key = self.format_key_with_size(&format!("rnd_{:016x}_{:016x}", i, random_part), key_size, &mut rng);
                    keys.push(key);
                }
            }
            KeyPattern::Uuid => {
                for _ in 0..count {
                    let uuid = format!("{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
                        rng.gen::<u32>(),
                        rng.gen::<u16>(),
                        rng.gen::<u16>(),
                        rng.gen::<u16>(),
                        rng.gen::<u64>() & 0xFFFFFFFFFFFF
                    );
                    let key = self.format_key_with_size(&uuid, key_size, &mut rng);
                    keys.push(key);
                }
            }
            KeyPattern::Hierarchical => {
                let levels = 3;
                let per_level = (count as f64).powf(1.0 / levels as f64).ceil() as usize;
                
                for i in 0..count {
                    let l1 = i / (per_level * per_level);
                    let l2 = (i / per_level) % per_level;
                    let l3 = i % per_level;
                    let key = self.format_key_with_size(&format!("hier/{:04x}/{:04x}/{:04x}", l1, l2, l3), key_size, &mut rng);
                    keys.push(key);
                }
            }
        }

        if matches!(pattern, KeyPattern::Random) {
            keys.shuffle(&mut rng);
        }

        keys
    }

    fn format_key_with_size(&self, base_key: &str, target_size: usize, rng: &mut StdRng) -> String {
        if target_size <= base_key.len() {
            base_key.chars().take(target_size).collect()
        } else {
            let mut key = base_key.to_string();
            let padding_needed = target_size - base_key.len();
            let padding: String = (0..padding_needed)
                .map(|_| char::from(b'a' + (rng.gen::<u8>() % 26)))
                .collect();
            key.push_str(&padding);
            key
        }
    }

    /// Generate values with different patterns
    pub fn generate_values(&self, count: usize, value_size: usize, pattern: ValuePattern) -> Vec<Vec<u8>> {
        let mut rng = self.seeded_rng(0x2000);
        let mut values = Vec::with_capacity(count);

        for i in 0..count {
            let value = match pattern {
                ValuePattern::Random => {
                    (0..value_size).map(|_| rng.gen()).collect()
                }
                ValuePattern::Sequential => {
                    let mut v = Vec::with_capacity(value_size);
                    v.extend_from_slice(&i.to_le_bytes());
                    while v.len() < value_size {
                        v.push((v.len() % 256) as u8);
                    }
                    v.truncate(value_size);
                    v
                }
                ValuePattern::Compressible => {
                    let pattern_byte = (i % 256) as u8;
                    let noise_ratio = 0.1;
                    (0..value_size).map(|j| {
                        if rng.gen::<f64>() < noise_ratio {
                            rng.gen()
                        } else {
                            pattern_byte.wrapping_add((j % 16) as u8)
                        }
                    }).collect()
                }
                ValuePattern::Json => {
                    let json_data = format!(
                        r#"{{"id": {}, "name": "object_{:08x}", "timestamp": {}, "data": "{}", "metrics": {{"cpu": {:.2}, "memory": {:.2}, "disk": {:.2}}}}}"#,
                        i,
                        i,
                        1600000000 + i,
                        (0..20).map(|_| char::from(b'a' + (rng.gen::<u8>() % 26))).collect::<String>(),
                        rng.gen::<f64>() * 100.0,
                        rng.gen::<f64>() * 100.0,
                        rng.gen::<f64>() * 100.0
                    );
                    let mut bytes = json_data.into_bytes();
                    bytes.resize(value_size, 0);
                    bytes
                }
            };
            values.push(value);
        }

        values
    }

    /// Generate variable length keys within a range
    pub fn generate_variable_keys(&self, count: usize, min_size: usize, max_size: usize) -> Vec<String> {
        let mut rng = self.seeded_rng(0x3000);
        let mut keys = Vec::with_capacity(count);

        for i in 0..count {
            let key_size = rng.gen_range(min_size..=max_size);
            let base_key = format!("var_{:08x}", i);
            let key = self.format_key_with_size(&base_key, key_size, &mut rng);
            keys.push(key);
        }

        keys
    }

    /// Generate temporal test data with multiple versions
    pub fn generate_temporal_data(&self, base_keys: &[String], versions_per_key: usize, value_size: usize) -> Vec<(String, Vec<u8>, u64)> {
        let mut data = Vec::new();
        let values = self.generate_values(base_keys.len() * versions_per_key, value_size, ValuePattern::Random);
        
        for (key_idx, key) in base_keys.iter().enumerate() {
            for version in 0..versions_per_key {
                let timestamp = 1_000_000 + (version as u64 * 1000) + (key_idx as u64);
                let value_idx = key_idx * versions_per_key + version;
                data.push((key.clone(), values[value_idx].clone(), timestamp));
            }
        }

        data
    }
}

#[derive(Clone, Debug)]
pub enum KeyPattern {
    Sequential,
    Random,
    Uuid,
    Hierarchical,
}

#[derive(Clone, Debug)]
pub enum ValuePattern {
    Random,
    Sequential,
    Compressible,
    Json,
}

/// Workload patterns for mixed benchmarks
#[derive(Clone, Debug)]
pub struct WorkloadPattern {
    pub read_percentage: usize,
    pub write_percentage: usize,
    pub temporal_read_percentage: usize,
    pub delete_percentage: usize,
}

impl WorkloadPattern {
    pub fn read_heavy() -> Self {
        Self {
            read_percentage: 80,
            write_percentage: 15,
            temporal_read_percentage: 5,
            delete_percentage: 0,
        }
    }

    pub fn write_heavy() -> Self {
        Self {
            read_percentage: 20,
            write_percentage: 70,
            temporal_read_percentage: 5,
            delete_percentage: 5,
        }
    }

    pub fn balanced() -> Self {
        Self {
            read_percentage: 50,
            write_percentage: 40,
            temporal_read_percentage: 8,
            delete_percentage: 2,
        }
    }

    pub fn temporal_heavy() -> Self {
        Self {
            read_percentage: 30,
            write_percentage: 20,
            temporal_read_percentage: 45,
            delete_percentage: 5,
        }
    }
}

/// Utility functions for benchmark setup
pub fn setup_large_dataset(config: &BenchmarkConfig, size_index: usize) -> (Vec<String>, Vec<Vec<u8>>) {
    let generator = DataGenerator::new(0xDEADBEEF);
    let size = config.dataset_sizes[size_index];
    
    let keys = generator.generate_keys(size, 64, KeyPattern::Random);
    let values = generator.generate_values(size, 512, ValuePattern::Random);
    
    (keys, values)
}

pub fn memory_usage_mb() -> f64 {
    #[cfg(target_os = "linux")]
    {
        use std::fs;
        if let Ok(status) = fs::read_to_string("/proc/self/status") {
            for line in status.lines() {
                if line.starts_with("VmRSS:") {
                    if let Some(kb_str) = line.split_whitespace().nth(1) {
                        if let Ok(kb) = kb_str.parse::<f64>() {
                            return kb / 1024.0;
                        }
                    }
                }
            }
        }
    }
    
    0.0 // Return 0 if we can't measure
}