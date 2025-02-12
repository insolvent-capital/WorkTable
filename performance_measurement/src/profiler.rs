use std::f64;
use std::time::{Duration, Instant};

use derive_more::Display;
use lazy_static::lazy_static;
use lockfree::map::Map;

lazy_static! {
    static ref GLOBAL_PERFORMANCE_MEASUREMENTS: Map<&'static str, PerformanceMeasurement> =
        Map::new();
}

#[derive(Copy, Clone, Debug, Display)]
#[display(
    "{}, max={:.6} min={:.6} avg={:.6}",
    name,
    max_duration,
    min_duration,
    avg_duration
)]
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
        let global_performance_measurements = &GLOBAL_PERFORMANCE_MEASUREMENTS;

        let duration_ms = duration.as_nanos() as f64 / 1_000_000.0;

        if let Some(guard) = global_performance_measurements.get(function_name) {
            let v = guard.val();
            let avg = ((v.avg_duration * v.measurement_count as f64) + duration_ms)
                / (v.measurement_count + 1) as f64;
            let m = PerformanceMeasurement {
                name: v.name,
                min_duration: f64::min(v.min_duration, duration_ms),
                max_duration: f64::max(v.max_duration, duration_ms),
                avg_duration: avg,
                measurement_count: v.measurement_count + 1,
            };
            global_performance_measurements.insert(function_name, m);
        } else {
            let m = PerformanceMeasurement {
                name: function_name,
                max_duration: duration_ms,
                min_duration: duration_ms,
                avg_duration: duration_ms,
                measurement_count: 1,
            };
            global_performance_measurements.insert(function_name, m);
        };
    }

    pub fn get_state<'a>() -> &'a Map<&'static str, PerformanceMeasurement> {
        &GLOBAL_PERFORMANCE_MEASUREMENTS
    }

    pub fn get_now() -> Instant {
        Instant::now()
    }
}
