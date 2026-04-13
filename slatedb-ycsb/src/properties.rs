use std::collections::HashMap;
use std::fs;
use std::path::Path;

use anyhow::{anyhow, Context, Result};
use tracing::warn;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Distribution {
    Uniform,
    Zipfian,
    Latest,
    Sequential,
    Hotspot,
}

impl Distribution {
    fn parse(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "uniform" => Ok(Self::Uniform),
            "zipfian" => Ok(Self::Zipfian),
            "latest" => Ok(Self::Latest),
            "sequential" => Ok(Self::Sequential),
            "hotspot" => Ok(Self::Hotspot),
            _ => Err(anyhow!("unsupported requestdistribution: {s}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InsertOrder {
    Hashed,
    Ordered,
}

impl InsertOrder {
    fn parse(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "hashed" => Ok(Self::Hashed),
            "ordered" => Ok(Self::Ordered),
            _ => Err(anyhow!("unsupported insertorder: {s}")),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Workload {
    pub(crate) record_count: u64,
    pub(crate) operation_count: u64,
    pub(crate) read_proportion: f64,
    pub(crate) update_proportion: f64,
    pub(crate) insert_proportion: f64,
    pub(crate) scan_proportion: f64,
    pub(crate) read_modify_write_proportion: f64,
    pub(crate) request_distribution: Distribution,
    pub(crate) max_scan_length: u64,
    pub(crate) scan_length_distribution: Distribution,
    pub(crate) field_count: usize,
    pub(crate) field_length: usize,
    pub(crate) insert_start: u64,
    pub(crate) insert_order: InsertOrder,
    pub(crate) zero_padding: usize,
    pub(crate) read_all_fields: bool,
    pub(crate) write_all_fields: bool,
    pub(crate) thread_count: u32,
    pub(crate) zipfian_constant: f64,
    pub(crate) hotspot_data_fraction: f64,
    pub(crate) hotspot_opn_fraction: f64,
}

impl Default for Workload {
    fn default() -> Self {
        Self {
            record_count: 1000,
            operation_count: 1000,
            read_proportion: 0.95,
            update_proportion: 0.05,
            insert_proportion: 0.0,
            scan_proportion: 0.0,
            read_modify_write_proportion: 0.0,
            request_distribution: Distribution::Uniform,
            max_scan_length: 1000,
            scan_length_distribution: Distribution::Uniform,
            field_count: 10,
            field_length: 100,
            insert_start: 0,
            insert_order: InsertOrder::Hashed,
            zero_padding: 1,
            read_all_fields: true,
            write_all_fields: false,
            thread_count: 1,
            zipfian_constant: 0.99,
            hotspot_data_fraction: 0.2,
            hotspot_opn_fraction: 0.8,
        }
    }
}

pub(crate) fn parse_properties_file(path: &Path) -> Result<HashMap<String, String>> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read properties file {}", path.display()))?;
    Ok(parse_properties_str(&content))
}

pub(crate) fn parse_properties_str(content: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for raw_line in content.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with('!') {
            continue;
        }
        let (k, v) = match line.find(['=', ':']) {
            Some(idx) => (line[..idx].trim(), line[idx + 1..].trim()),
            None => (line, ""),
        };
        map.insert(k.to_string(), v.to_string());
    }
    map
}

impl Workload {
    pub(crate) fn from_properties(props: &HashMap<String, String>) -> Result<Self> {
        let mut w = Workload::default();
        for (k, v) in props {
            match k.as_str() {
                "recordcount" => w.record_count = v.parse().context("recordcount")?,
                "operationcount" => w.operation_count = v.parse().context("operationcount")?,
                "readproportion" => w.read_proportion = v.parse().context("readproportion")?,
                "updateproportion" => {
                    w.update_proportion = v.parse().context("updateproportion")?
                }
                "insertproportion" => {
                    w.insert_proportion = v.parse().context("insertproportion")?
                }
                "scanproportion" => w.scan_proportion = v.parse().context("scanproportion")?,
                "readmodifywriteproportion" => {
                    w.read_modify_write_proportion =
                        v.parse().context("readmodifywriteproportion")?
                }
                "requestdistribution" => w.request_distribution = Distribution::parse(v)?,
                "maxscanlength" => w.max_scan_length = v.parse().context("maxscanlength")?,
                "scanlengthdistribution" => w.scan_length_distribution = Distribution::parse(v)?,
                "fieldcount" => w.field_count = v.parse().context("fieldcount")?,
                "fieldlength" => w.field_length = v.parse().context("fieldlength")?,
                "insertstart" => w.insert_start = v.parse().context("insertstart")?,
                "insertorder" => w.insert_order = InsertOrder::parse(v)?,
                "zeropadding" => w.zero_padding = v.parse().context("zeropadding")?,
                "readallfields" => w.read_all_fields = v.parse().context("readallfields")?,
                "writeallfields" => w.write_all_fields = v.parse().context("writeallfields")?,
                "threadcount" => w.thread_count = v.parse().context("threadcount")?,
                "zipfian_constant" | "zipfianconstant" => {
                    w.zipfian_constant = v.parse().context("zipfian_constant")?
                }
                "hotspotdatafraction" => {
                    w.hotspot_data_fraction = v.parse().context("hotspotdatafraction")?
                }
                "hotspotopnfraction" => {
                    w.hotspot_opn_fraction = v.parse().context("hotspotopnfraction")?
                }
                "workload" | "table" | "measurementtype" | "dotransactions" => {}
                other => warn!("ignoring unknown YCSB property: {other}={v}"),
            }
        }
        w.validate()?;
        Ok(w)
    }

    fn validate(&self) -> Result<()> {
        let total = self.read_proportion
            + self.update_proportion
            + self.insert_proportion
            + self.scan_proportion
            + self.read_modify_write_proportion;
        if total <= 0.0 {
            return Err(anyhow!(
                "operation proportions sum to zero; at least one op type must have a non-zero proportion"
            ));
        }
        if (total - 1.0).abs() > 0.01 {
            warn!("operation proportions sum to {total:.3}, not 1.0 — normalizing at runtime");
        }
        if self.field_count == 0 {
            return Err(anyhow!("fieldcount must be >= 1"));
        }
        if self.field_length == 0 {
            return Err(anyhow!("fieldlength must be >= 1"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_workloada() {
        let src = "\
# Test
recordcount=1000
operationcount=1000
workload=site.ycsb.workloads.CoreWorkload
readproportion=0.5
updateproportion=0.5
scanproportion=0
insertproportion=0
requestdistribution=zipfian
";
        let props = parse_properties_str(src);
        let w = Workload::from_properties(&props).unwrap();
        assert_eq!(w.record_count, 1000);
        assert_eq!(w.operation_count, 1000);
        assert!((w.read_proportion - 0.5).abs() < 1e-9);
        assert!((w.update_proportion - 0.5).abs() < 1e-9);
        assert_eq!(w.request_distribution, Distribution::Zipfian);
    }

    #[test]
    fn parses_workloade_scans() {
        let src = "\
readproportion=0.95
scanproportion=0.05
updateproportion=0
requestdistribution=zipfian
maxscanlength=100
scanlengthdistribution=uniform
";
        let props = parse_properties_str(src);
        let w = Workload::from_properties(&props).unwrap();
        assert!((w.scan_proportion - 0.05).abs() < 1e-9);
        assert_eq!(w.max_scan_length, 100);
        assert_eq!(w.scan_length_distribution, Distribution::Uniform);
    }

    #[test]
    fn rejects_all_zero_proportions() {
        let src = "readproportion=0\nupdateproportion=0\n";
        let props = parse_properties_str(src);
        assert!(Workload::from_properties(&props).is_err());
    }

    #[test]
    fn handles_colon_and_comments() {
        let src = "# comment\n! bang comment\nrecordcount: 42\n";
        let props = parse_properties_str(src);
        assert_eq!(props.get("recordcount").map(String::as_str), Some("42"));
    }
}
