use pyo3::{exceptions, PyErr, PyResult};
use pyo3::create_exception;
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use numpy::{IntoPyArray, PyArrayDyn};
use std::fs::OpenOptions;
use crate::errors::*;
use crate::ORCFile;
use std::io::{Read, Seek};

trait ReadSeek : Read + Seek + Send {}
impl<T: Read + Seek + Send> ReadSeek for T {}

#[pyclass]
struct PyOrcFile{
    inner: ORCFile<Box<dyn ReadSeek>>
}
#[pymethods]
impl PyOrcFile {
    #[new]
    fn new(filename: Option<&str>, content: Option<&[u8]>) -> PyResult<Self> {
        match (filename, content) {
            (None, None) => Err(OrcError::InvalidArgumentException("Must specify either filename or content").into()),
            (Some(filename), None) => {
                let file = OpenOptions::new().read(true).open(filename)?;
                let file: Box<dyn ReadSeek> = Box::new(file);
                let file = ORCFile::from_reader(file)?;
                Ok(PyOrcFile{inner: file})
            },
            (None, Some(_)) => Err(OrcError::InvalidArgumentException("Passing content bytes is not supported yet").into()),
            (Some(_), Some(_)) => Err(OrcError::InvalidArgumentException("Can't pass both filename and content").into()),
        }
    }

    /// Read a stripe from the ORC File
    pub fn stripe(&mut self, stripe_id: usize) -> PyResult<PyStripe> {
        Ok(PyStripe{ inner: self.inner.stripe(stripe_id)? })
    } 
}

#[pyclass]
struct PyStripe {
    inner: crate::toc::Stripe
}
#[pymethods]
impl PyStripe {
    pub fn dataframe<'py>(
        &self,
        py: Python<'py>,
        toc_cell: &PyCell<PyOrcFile>
    ) -> PyResult<Vec<&'py PyArrayDyn<f32>>> {
        let mut toc = toc_cell.try_borrow_mut()?;
        let df = self.inner.dataframe(&mut (*toc).inner)?;
        let mut columns = vec![];
        for col in &df.column_order {
            match df.columns[col].valid().to_numbers::<f32>() {
                Ok(numbers) => columns.push(numbers.into_pyarray(py).to_dyn()),
                Err(e) => println!("Error reading column: {}", e)
            };
        }
        Ok(columns)
    } 
}

#[pyfunction]
/// Formats the sum of two numbers as string.
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

#[pymodule]
/// A Python module implemented in Rust.
pub fn orc_format(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(sum_as_string))?;
    m.add_class::<PyOrcFile>()?;
    Ok(())
}

//
// Python Error Handling
//
create_exception!(orc_format, PyOrcError, pyo3::exceptions::PyException);

impl std::convert::From<OrcError> for PyErr {
    fn from(err: OrcError) -> PyErr {
        PyOrcError::new_err(err.to_string())
    }
}