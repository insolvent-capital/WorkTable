use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use derive_more::Display;
use lazy_static::lazy_static;

lazy_static! {
    static ref GLOBAL_PERFORMANCE_MEASUREMENTS: Mutex<HashMap<&'static str, PerformanceMeasurement>> =
        Mutex::new(HashMap::new());
}

#[derive(Copy, Clone, Debug, Display)]
#[display("{}, max={:.6} min={:.6} avg={:.6}", name, max_duration, min_duration, avg_duration)]
pub struct PerformanceMeasurement {
    pub name: &'static str,

    pub max_duration: f64,
    pub min_duration: f64,

    pub avg_duration: f64,

    pub measurement_count: u64,
}

//  Performance profiling manager
//  Updates and reads [`PerformanceMeasurement`] in [`GLOBAL_PERFORMANCE_MEASUREMENTS`]
//  Used to gather and provide data about application performance
pub struct PerformanceProfiler;

impl PerformanceProfiler {
    pub fn store_measurement(function_name: &'static str, duration: Duration) {
        let mut global_performance_measurements = GLOBAL_PERFORMANCE_MEASUREMENTS.lock().unwrap();

        let duration_ms = duration.as_nanos() as f64 / 1_000_000.0;

        global_performance_measurements.entry(function_name).and_modify(|v| {
            if duration_ms > v.max_duration {
                v.max_duration = duration_ms
            }
            if duration_ms < v.min_duration {
                v.min_duration = duration_ms
            }
            v.avg_duration = ((v.avg_duration * v.measurement_count as f64) + duration_ms) / (v.measurement_count + 1) as f64;
            v.measurement_count += 1;
        }).or_insert(PerformanceMeasurement {
            name: function_name,
            max_duration: duration_ms,
            min_duration: duration_ms,
            avg_duration: duration_ms,
            measurement_count: 1
        });
    }

    pub fn get_now() -> Instant {
        Instant::now()
    }
}