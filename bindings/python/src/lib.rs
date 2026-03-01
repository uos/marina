use ::marina as marina_rs;
use marina_rs::{Marina, ProgressEvent, ProgressReporter, ProgressSink, ResolveResult, WriterProgress};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyAnyMethods;
use pyo3::types::PyModuleMethods;

#[pyclass]
struct ResolveDetailed {
    #[pyo3(get)]
    kind: String,
    #[pyo3(get)]
    path: Option<String>,
    #[pyo3(get)]
    bag: Option<String>,
    #[pyo3(get)]
    registry: Option<String>,
    #[pyo3(get)]
    message: Option<String>,
}

#[pymethods]
impl ResolveDetailed {
    #[getter]
    fn should_pull(&self) -> bool {
        self.kind == "remote_available"
    }
}

#[pyfunction]
fn resolve(target: &str) -> PyResult<String> {
    let marina = Marina::load().map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match marina
        .resolve_target(target)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
    {
        ResolveResult::LocalPath(p) | ResolveResult::Cached(p) => Ok(p.display().to_string()),
        ResolveResult::RemoteAvailable { bag, registry, .. } => {
            Ok(format!("REMOTE:{}@{}", bag, registry))
        }
    }
}

#[pyfunction]
#[pyo3(signature = (bag_ref, registry=None))]
fn pull(bag_ref: &str, registry: Option<&str>) -> PyResult<String> {
    let bag = bag_ref
        .parse::<marina_rs::BagRef>()
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let mut marina = Marina::load().map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let pulled = marina
        .pull_exact(&bag, registry)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    Ok(pulled.display().to_string())
}

struct PyWriterProgressSink {
    writer: Py<PyAny>,
}

impl ProgressSink for PyWriterProgressSink {
    fn emit(&mut self, event: ProgressEvent) {
        Python::with_gil(|py| {
            let writer = self.writer.bind(py);
            let line = format!("[{}] {}\n", event.phase, event.message);
            let _ = writer.call_method1("write", (line,));
            let _ = writer.call_method0("flush");
        });
    }
}

#[pyfunction]
#[pyo3(signature = (bag_ref, registry=None, progress=false, writer=None))]
fn pull_with_progress(
    bag_ref: &str,
    registry: Option<&str>,
    progress: bool,
    writer: Option<Py<PyAny>>,
) -> PyResult<String> {
    let bag = bag_ref
        .parse::<marina_rs::BagRef>()
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let mut marina = Marina::load().map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    let pulled = if let Some(writer) = writer {
        let mut sink = PyWriterProgressSink { writer };
        let mut reporter = ProgressReporter::new(&mut sink);
        marina.pull_exact_with_progress(&bag, registry, &mut reporter)
    } else if progress {
        let mut stdout = std::io::stdout();
        let mut sink = WriterProgress::new(&mut stdout);
        let mut reporter = ProgressReporter::new(&mut sink);
        marina.pull_exact_with_progress(&bag, registry, &mut reporter)
    } else {
        marina.pull_exact(&bag, registry)
    }
    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    Ok(pulled.display().to_string())
}

#[pyfunction]
fn resolve_detailed(target: &str) -> ResolveDetailed {
    let marina = match Marina::load() {
        Ok(v) => v,
        Err(e) => {
            return ResolveDetailed {
                kind: "error".to_string(),
                path: None,
                bag: None,
                registry: None,
                message: Some(format!("failed to load marina: {e}")),
            }
        }
    };

    match marina.resolve_target(target) {
        Ok(ResolveResult::LocalPath(p)) => ResolveDetailed {
            kind: "local".to_string(),
            path: Some(p.display().to_string()),
            bag: None,
            registry: None,
            message: Some("local path resolved".to_string()),
        },
        Ok(ResolveResult::Cached(p)) => ResolveDetailed {
            kind: "cached".to_string(),
            path: Some(p.display().to_string()),
            bag: None,
            registry: None,
            message: Some("cached path resolved".to_string()),
        },
        Ok(ResolveResult::RemoteAvailable { bag, registry, .. }) => ResolveDetailed {
            kind: "remote_available".to_string(),
            path: None,
            bag: Some(bag.to_string()),
            registry: Some(registry),
            message: Some("remote bag available; call pull(...)".to_string()),
        },
        Err(e) => ResolveDetailed {
            kind: "error".to_string(),
            path: None,
            bag: None,
            registry: None,
            message: Some(e.to_string()),
        },
    }
}

#[pyfunction]
fn cli_main(py: Python<'_>) -> PyResult<()> {
    let sys = py.import("sys")?;
    let argv: Vec<String> = sys.getattr("argv")?.extract()?;
    let args = if argv.is_empty() {
        vec!["marina".to_string()]
    } else {
        argv
    };
    marina_rs::cli::app::run_with_args(args).map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

#[pymodule]
fn marina(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ResolveDetailed>()?;
    m.add_function(wrap_pyfunction!(resolve, m)?)?;
    m.add_function(wrap_pyfunction!(resolve_detailed, m)?)?;
    m.add_function(wrap_pyfunction!(pull, m)?)?;
    m.add_function(wrap_pyfunction!(pull_with_progress, m)?)?;
    m.add_function(wrap_pyfunction!(cli_main, m)?)?;
    Ok(())
}
