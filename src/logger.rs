use log::Log;
use log::Metadata;
use log::Record;
use std::sync::Once;

static START: Once = Once::new();

struct SimpleLogger;

impl Log for SimpleLogger {
    fn enabled(&self, _metadata: &Metadata) -> bool {
        true
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            println!("{} - {}", record.level(), record.args());
        }
    }

    fn flush(&self) {}
}

pub(crate) fn init() {
    START.call_once(|| {
        ::log::set_logger(&SimpleLogger).expect("This logger needs to be present");
    });
}
