use super::{Event, Operator};

/// Terminal operator that prints every received event to stdout.
///
/// Each line shows the logical label given at construction time, the diff
/// (+1 insert / -1 delete), and the full record contents.
pub struct SinkOp {
    label: String,
}

impl SinkOp {
    pub fn new(label: impl Into<String>) -> Self {
        SinkOp { label: label.into() }
    }
}

impl Operator for SinkOp {
    fn on_event(&mut self, event: Event) {
        // Sort columns for deterministic output.
        let mut cols: Vec<_> = event.record.iter().collect();
        cols.sort_by_key(|(k, _)| k.as_str());
        let row: Vec<_> = cols.iter().map(|(k, v)| format!("{k}={v:?}")).collect();
        println!(
            "[{}] weight={:+}  {{ {} }}",
            self.label,
            event.weight.0,
            row.join(", ")
        );
    }
}
