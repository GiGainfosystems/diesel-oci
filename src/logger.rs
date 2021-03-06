use log::{Log, LogLevelFilter, LogMetadata as Metadata, LogRecord as Record};
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
}

pub(crate) fn init() {
    START.call_once(|| {
        ::log::set_logger(|max_log_level| {
            max_log_level.set(LogLevelFilter::Debug);
            Box::new(SimpleLogger)
        }).expect("This logger needs to be present");
    });
}
