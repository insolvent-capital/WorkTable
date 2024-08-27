use performance_measurement::performance_measurement;

#[performance_measurement(prefix_name = "MeasureMe")]
fn measure_me() {

}

#[performance_measurement(prefix_name = "MeasureMe")]
fn count(a: u64, b: u64) -> u64 {
    a + b
}

struct Test {
    a: u64
}

impl Test {
    #[performance_measurement(prefix_name = "MeasureMe")]
    pub fn test(&self, b: u64) -> u64 {
        self.a + b
    }
}


#[test]
fn test() {
    count(1, 2);
    count(2, 3);
    count(4, 5);

    let t = Test {
        a: 1
    };

    t.test(2);
    t.test(3);
    t.test(4);
}