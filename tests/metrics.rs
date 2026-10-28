//! The collection's metrics, read through an SDK meter provider installed before the collection opens (its own
//! process, so no other test makes the instruments first).

use std::io;
use std::sync::Arc;

use invertdb_collection::{Collection, Domain, codec};
use introvert::{Index, Options, Snapshot};
use larry::Store;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};

/// Records are a key and its words; nothing beside the index.
struct Words;

impl Domain for Words {
    type Key = String;
    type Unit = String;
    type Record = (String, Vec<String>);
    type Side = ();

    fn index_options(&self) -> Options {
        Options { frequencies: false, positions: false, ..Options::default() }
    }

    fn encode(&self, (key, words): &Self::Record, out: &mut Vec<u8>) {
        codec::write_string(out, key);
        codec::write_varint(out, words.len() as u64);
        for w in words {
            codec::write_string(out, w);
        }
    }

    fn decode(&self, bytes: &[u8]) -> Option<Self::Record> {
        let at = &mut 0;
        let key = codec::read_string(bytes, at)?;
        let n = codec::read_varint(bytes, at);
        Some((key, (0..n).map(|_| codec::read_string(bytes, at)).collect::<Option<Vec<_>>>()?))
    }

    fn key(&self, (key, _): &Self::Record) -> String {
        key.clone()
    }

    fn open(&self, _: &Snapshot<String, String>) -> io::Result<()> {
        Ok(())
    }

    fn apply(&self, batch: &[Self::Record], _: u32, _: &(), index: &Index<String, String>) -> io::Result<()> {
        for (key, words) in batch {
            index.add(key, words.iter().map(|w| (w.clone(), 0)));
        }
        Ok(())
    }
}

/// A metric's value for the collection `name`: a gauge's last value or a sum's or histogram's count.
fn value(metrics: &[ResourceMetrics], metric: &str, name: &str) -> Option<f64> {
    let mut found = None;
    for rm in metrics {
        for sm in rm.scope_metrics() {
            for m in sm.metrics().filter(|m| m.name() == metric) {
                let ours = |mut attributes: Box<dyn Iterator<Item = &opentelemetry::KeyValue> + '_>| attributes.any(|kv| kv.key.as_str() == "invertdb.collection" && kv.value.as_str() == name);
                match m.data() {
                    AggregatedMetrics::U64(MetricData::Gauge(g)) => {
                        for p in g.data_points() {
                            if ours(Box::new(p.attributes())) {
                                found = Some(p.value() as f64);
                            }
                        }
                    }
                    AggregatedMetrics::U64(MetricData::Sum(s)) => {
                        let total: u64 = s.data_points().filter(|p| ours(Box::new(p.attributes()))).map(|p| p.value()).sum();
                        found = Some(total as f64);
                    }
                    AggregatedMetrics::F64(MetricData::Histogram(h)) => {
                        for p in h.data_points() {
                            if ours(Box::new(p.attributes())) {
                                found = Some(p.count() as f64);
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    found
}

#[test]
fn a_collection_reports_its_metrics() {
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder().with_reader(PeriodicReader::builder(exporter.clone()).build()).build();
    opentelemetry::global::set_meter_provider(provider.clone());

    let dir = tempfile::tempdir().unwrap();
    let files = Store::open(dir.path().join("notes")).unwrap();
    let c = Arc::new(Collection::open(&files, Words).unwrap());
    c.add((0..5).map(|i| (format!("k{i}"), vec!["w".to_string()])).collect()).unwrap();
    c.delete(vec!["k0".into(), "k1".into()]).unwrap();
    c.commit().unwrap();
    c.add(vec![("k9".into(), vec!["w".into()])]).unwrap();

    provider.force_flush().unwrap();
    let metrics = exporter.get_finished_metrics().unwrap();
    let get = |metric: &str| value(&metrics, metric, "notes");
    assert_eq!(get("invertdb.collection.sources"), Some(3.0), "live sources");
    assert_eq!(get("invertdb.collection.deleted"), Some(2.0));
    assert_eq!(get("invertdb.collection.pending"), Some(1.0));
    assert_eq!(get("invertdb.collection.added"), Some(5.0));
    assert_eq!(get("invertdb.collection.removed"), Some(2.0));
    assert_eq!(get("invertdb.collection.operations"), Some(8.0), "5 adds, 2 deletes, 1 add");
    assert_eq!(get("invertdb.collection.commit.duration"), Some(1.0), "one commit");
    assert!(get("invertdb.collection.disk").unwrap() > 0.0);
    // and the layers below, through the same provider
    let below = |metric: &str| metrics.iter().flat_map(|rm| rm.scope_metrics()).flat_map(|sm| sm.metrics()).any(|m| m.name() == metric);
    assert!(below("invertdb.log.sync.duration") && below("invertdb.index.commit.duration") && below("invertdb.log.appended"));
}
