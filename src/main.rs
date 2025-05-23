use std::time::Instant;
use std::io::{self, Write};
use zverse::{ZVerse, ZVerseConfig, Error};

/// Print a usage message
fn print_usage() {
    println!("ZVerse - Z-Order Curve Optimized Versioned KV Store");
    println!("Usage:");
    println!("  zverse [OPTIONS] COMMAND [ARGS]");
    println!("");
    println!("Options:");
    println!("  --path PATH       Data directory path (default: ./zverse_data)");
    println!("  --segment-size N  Segment size in MB (default: 16)");
    println!("  --sync            Sync writes immediately (default: true)");
    println!("  --cache-size N    Cache size in MB (default: 1024)");
    println!("  --help            Show this help message");
    println!("");
    println!("Commands:");
    println!("  get KEY [VERSION]           Get value for key at version (default: latest)");
    println!("  put KEY VALUE               Put value for key");
    println!("  delete KEY                  Delete key");
    println!("  scan [START] [END] [VERSION] Scan range of keys at version (default: latest)");
    println!("  history KEY [START] [END]   Get history of key from start to end version");
    println!("  benchmark                   Run basic benchmark");
    println!("  version                     Show version information");
}

/// Parse command line arguments
fn parse_args() -> Result<(ZVerseConfig, String, Vec<String>), String> {
    let mut args = std::env::args().skip(1).collect::<Vec<_>>();
    
    if args.is_empty() || args.contains(&"--help".to_string()) {
        print_usage();
        std::process::exit(0);
    }
    
    // Default configuration
    let mut config = ZVerseConfig::default();
    
    // Parse options
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--path" => {
                if i + 1 < args.len() {
                    config.data_path = args[i + 1].clone();
                    args.remove(i);
                    args.remove(i);
                } else {
                    return Err("Missing value for --path".to_string());
                }
            }
            "--segment-size" => {
                if i + 1 < args.len() {
                    let size = args[i + 1].parse::<usize>()
                        .map_err(|_| "Invalid segment size".to_string())?;
                    config.segment_size = size * 1024 * 1024;
                    args.remove(i);
                    args.remove(i);
                } else {
                    return Err("Missing value for --segment-size".to_string());
                }
            }
            "--sync" => {
                config.sync_writes = true;
                args.remove(i);
            }
            "--no-sync" => {
                config.sync_writes = false;
                args.remove(i);
            }
            "--cache-size" => {
                if i + 1 < args.len() {
                    let size = args[i + 1].parse::<usize>()
                        .map_err(|_| "Invalid cache size".to_string())?;
                    config.cache_size_bytes = size * 1024 * 1024;
                    args.remove(i);
                    args.remove(i);
                } else {
                    return Err("Missing value for --cache-size".to_string());
                }
            }
            _ => {
                i += 1;
            }
        }
    }
    
    if args.is_empty() {
        return Err("Missing command".to_string());
    }
    
    let command = args[0].clone();
    let command_args = args.into_iter().skip(1).collect();
    
    Ok((config, command, command_args))
}

/// Handle get command
fn handle_get(db: &ZVerse, args: &[String]) -> Result<(), Error> {
    if args.is_empty() {
        return Err(Error::Other("Missing key argument".to_string()));
    }
    
    let key = args[0].as_bytes();
    let version = if args.len() > 1 {
        Some(args[1].parse::<u64>().map_err(|_| Error::Other("Invalid version".to_string()))?)
    } else {
        None
    };
    
    match db.get::<_, Vec<u8>>(key, version)? {
        Some(value) => {
            println!("Value: {}", String::from_utf8_lossy(&value));
            Ok(())
        }
        None => {
            println!("Key not found");
            Ok(())
        }
    }
}

/// Handle put command
fn handle_put(db: &ZVerse, args: &[String]) -> Result<(), Error> {
    if args.len() < 2 {
        return Err(Error::Other("Missing key or value argument".to_string()));
    }
    
    let key = args[0].as_bytes();
    let value = args[1].as_bytes();
    
    let version = db.put(key, value)?;
    println!("Put successful at version {}", version);
    
    Ok(())
}

/// Handle delete command
fn handle_delete(db: &ZVerse, args: &[String]) -> Result<(), Error> {
    if args.is_empty() {
        return Err(Error::Other("Missing key argument".to_string()));
    }
    
    let key = args[0].as_bytes();
    
    let version = db.delete(key)?;
    println!("Delete successful at version {}", version);
    
    Ok(())
}

/// Handle scan command
fn handle_scan(db: &ZVerse, args: &[String]) -> Result<(), Error> {
    let start = if !args.is_empty() && args[0] != "-" {
        Some(args[0].as_bytes())
    } else {
        None
    };
    
    let end = if args.len() > 1 && args[1] != "-" {
        Some(args[1].as_bytes())
    } else {
        None
    };
    
    let version = if args.len() > 2 {
        Some(args[2].parse::<u64>().map_err(|_| Error::Other("Invalid version".to_string()))?)
    } else {
        None
    };
    
    let iter = db.scan(start, end, version)?;
    let mut count = 0;
    
    for result in iter {
        let (key, value) = result?;
        println!("Key: {}, Value: {}", 
                 String::from_utf8_lossy(&key),
                 String::from_utf8_lossy(&value));
        count += 1;
    }
    
    println!("Total: {} records", count);
    
    Ok(())
}

/// Handle history command
fn handle_history(db: &ZVerse, args: &[String]) -> Result<(), Error> {
    if args.is_empty() {
        return Err(Error::Other("Missing key argument".to_string()));
    }
    
    let key = args[0].as_bytes();
    
    let start_version = if args.len() > 1 && args[1] != "-" {
        Some(args[1].parse::<u64>().map_err(|_| Error::Other("Invalid start version".to_string()))?)
    } else {
        None
    };
    
    let end_version = if args.len() > 2 && args[2] != "-" {
        Some(args[2].parse::<u64>().map_err(|_| Error::Other("Invalid end version".to_string()))?)
    } else {
        None
    };
    
    let iter = db.history(key, start_version, end_version)?;
    let mut count = 0;
    
    for result in iter {
        let (version, value) = result?;
        println!("Version: {}, Value: {}", version, String::from_utf8_lossy(&value));
        count += 1;
    }
    
    println!("Total: {} versions", count);
    
    Ok(())
}

/// Handle benchmark command
fn handle_benchmark(db: &ZVerse, _args: &[String]) -> Result<(), Error> {
    println!("Running basic benchmark...");
    
    // Parameters
    const NUM_KEYS: usize = 100_000;
    const KEY_SIZE: usize = 16;
    const VALUE_SIZE: usize = 100;
    
    // Generate random data
    println!("Generating {} random key-value pairs...", NUM_KEYS);
    let mut keys = Vec::with_capacity(NUM_KEYS);
    let mut values = Vec::with_capacity(NUM_KEYS);
    
    for i in 0..NUM_KEYS {
        let mut key = Vec::with_capacity(KEY_SIZE);
        key.extend_from_slice(format!("key-{:010}", i).as_bytes());
        keys.push(key);
        
        let mut value = Vec::with_capacity(VALUE_SIZE);
        value.extend_from_slice(format!("value-{:0100}", i).as_bytes());
        values.push(value);
    }
    
    // Benchmark put
    println!("Benchmarking put...");
    let start = Instant::now();
    for i in 0..NUM_KEYS {
        db.put(&keys[i], &values[i])?;
        
        if (i + 1) % 10_000 == 0 {
            print!("\r{}/{} keys written", i + 1, NUM_KEYS);
            io::stdout().flush().unwrap();
        }
    }
    let put_duration = start.elapsed();
    println!("\nPut: {} keys in {:?} ({:.2} keys/sec)",
             NUM_KEYS,
             put_duration,
             NUM_KEYS as f64 / put_duration.as_secs_f64());
    
    // Benchmark get
    println!("Benchmarking get...");
    let start = Instant::now();
    for i in 0..NUM_KEYS {
        db.get::<_, Vec<u8>>(&keys[i], None)?;
        
        if (i + 1) % 10_000 == 0 {
            print!("\r{}/{} keys read", i + 1, NUM_KEYS);
            io::stdout().flush().unwrap();
        }
    }
    let get_duration = start.elapsed();
    println!("\nGet: {} keys in {:?} ({:.2} keys/sec)",
             NUM_KEYS,
             get_duration,
             NUM_KEYS as f64 / get_duration.as_secs_f64());
    
    // Benchmark scan
    println!("Benchmarking scan...");
    let start = Instant::now();
    let iter = db.scan::<&[u8]>(None, None, None)?;
    let mut count = 0;
    for _ in iter {
        count += 1;
    }
    let scan_duration = start.elapsed();
    println!("Scan: {} keys in {:?} ({:.2} keys/sec)",
             count,
             scan_duration,
             count as f64 / scan_duration.as_secs_f64());
    
    Ok(())
}

/// Handle version command
fn handle_version(_db: &ZVerse, _args: &[String]) -> Result<(), Error> {
    println!("ZVerse v{}", env!("CARGO_PKG_VERSION"));
    println!("Z-Order Curve Optimized Versioned KV Store");
    println!("https://github.com/surrealdb/zverse");
    
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse command line arguments
    let (config, command, args) = match parse_args() {
        Ok(result) => result,
        Err(err) => {
            eprintln!("Error: {}", err);
            print_usage();
            std::process::exit(1);
        }
    };
    
    // Create ZVerse instance
    let db = ZVerse::new(config)?;
    
    // Dispatch command
    let result = match command.as_str() {
        "get" => handle_get(&db, &args),
        "put" => handle_put(&db, &args),
        "delete" => handle_delete(&db, &args),
        "scan" => handle_scan(&db, &args),
        "history" => handle_history(&db, &args),
        "benchmark" => handle_benchmark(&db, &args),
        "version" => handle_version(&db, &args),
        _ => {
            eprintln!("Unknown command: {}", command);
            print_usage();
            std::process::exit(1);
        }
    };
    
    // Handle errors
    if let Err(err) = result {
        eprintln!("Error: {}", err);
        std::process::exit(1);
    }
    
    Ok(())
}
