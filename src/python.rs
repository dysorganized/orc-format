use crate::errors::*;
use crate::schemata::{Column, NullableColumn};
use crate::ORCFile;
use numpy::{IntoPyArray, PyArray1};
use pyo3::{create_exception, types::{IntoPyDict, PyBytes, PyDict, PyTuple}};
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use pyo3::{exceptions, PyErr, PyResult};
use std::fs::OpenOptions;
use std::io::{Read, Seek};

trait ReadSeek: Read + Seek + Send {}
impl<T: Read + Seek + Send> ReadSeek for T {}

#[pyclass]
struct PyOrcFile {
    inner: ORCFile<Box<dyn ReadSeek>>,
}
#[pymethods]
impl PyOrcFile {
    #[new]
    fn new(filename: Option<&str>, content: Option<&[u8]>) -> PyResult<Self> {
        match (filename, content) {
            (None, None) => Err(OrcError::InvalidArgumentException(
                "Must specify either filename or content",
            )
            .into()),
            (Some(filename), None) => {
                let file = OpenOptions::new().read(true).open(filename)?;
                let file: Box<dyn ReadSeek> = Box::new(file);
                let file = ORCFile::from_reader(file)?;
                Ok(PyOrcFile { inner: file })
            }
            (None, Some(_)) => Err(OrcError::InvalidArgumentException(
                "Passing content bytes is not supported yet",
            )
            .into()),
            (Some(_), Some(_)) => Err(OrcError::InvalidArgumentException(
                "Can't pass both filename and content",
            )
            .into()),
        }
    }

    /// Get the number of stripes in this ORC file
    pub fn stripe_count(&self) -> usize {
        self.inner.stripe_count()
    }

    /// Read a stripe from the ORC File
    pub fn stripe(&mut self, stripe_id: usize) -> PyResult<PyStripe> {
        Ok(PyStripe {
            inner: self.inner.stripe(stripe_id)?,
        })
    }
}

#[pyclass]
struct PyStripe {
    inner: crate::toc::Stripe,
}
#[pymethods]
impl PyStripe {
    /// Return how many columns there are in this stripe.
    /// It should be the same for all the stripes.
    pub fn cols(&self) -> usize {
        self.inner.cols()
    }

    /// Read the column from the stripe.
    ///
    /// It isn't directly usable in Python at this point and still needs to be converted
    /// using one of the included methods
    pub fn column(&self, id: usize, toc_cell: &PyCell<PyOrcFile>) -> PyResult<PyNullableColumn> {
        let mut toc = toc_cell.try_borrow_mut()?;
        Ok(PyNullableColumn {
            inner: self.inner.column(id, &mut (*toc).inner)?,
        })
    }

    // /// Read the whole stripe into a dataframe or similar object
    // ///
    // /// Accepts
    // /// -------
    // /// * toc_cell: a PyORCFile to read this dataframe from
    // /// * keep_columns: list[str]: names of columns to keep
    // ///                            or if None, keep all columns
    // /// * pack_blobs: bool: Whether to pack blobs, conflicts with pandas
    // /// * as_pandas: bool: whether to return a pandas dataframe
    // ///
    // /// Notes
    // /// -----
    // /// Integral and floating point columns are pretty fast.
    // /// Strings
    // pub fn dataframe<'py>(
    //     &self,
    //     py: Python<'py>,
    //     toc_cell: &PyCell<PyOrcFile>,
    //     keep_columns: Option<Vec<String>>,
    //     as_pandas: bool
    // ) -> PyResult<PyObject> {
    //     // This function is somewhat involved because we need help from Numpy
    //     // to generate the masked arrays we want.
    //     // We also need Pandas to convert it to a dataframe when we are done.

    //     // Python objects are all checked at runtime so they have these cells
    //     // In this case we need a reference to the original file to be able
    //     // to read it, because sharing a file is not really threadsafe
    //     // (seek will effect all threads)
    //     let mut toc = toc_cell.try_borrow_mut()?;
    //     let rust_df = self.inner.dataframe(&mut (*toc).inner)?;

    //     // We want to convert the columns to arrays and masks.
    //     // The arrays need to be dynamically typed so we cast them to PyObject
    //     // Then we convert them to masked arrays (inside python)
    //     // And finally convert all that to a pandas dataframe
    //     let np = py.import("numpy")?;

    //     let mut columns: Vec<(&str, PyObject)> = vec![];
    //     for col_name in &rust_df.column_order {
    //         let col = &rust_df.columns[col_name];
    //         let present = col.present_or_trues();
    //         let content : PyObject = match col.content_or_default() {
    //             Column::Byte { data, .. } => data.into_pyarray(py).into(),
    //             Column::Boolean { data, .. } => data.into_pyarray(py).into(),
    //             Column::Int { data, .. } => data
    //                 .iter()
    //                 .map(|x| *x as i64)
    //                 .collect::<Vec<_>>()
    //                 .into_pyarray(py)
    //                 .into(),
    //             Column::Float { data, .. } => data.clone().into_pyarray(py).into(),
    //             Column::Double { data, .. } => data.clone().into_pyarray(py).into(),
    //             // Column::Blob { data, length, utf8} => {
    //             //     if utf8 {
    //             //         let v: Vec<String> = vec![];
    //             //         let mut start = 0;
    //             //         for len in length {
    //             //             let len = len as usize;
    //             //             v.push(&data[start..start + len].to_owned());
    //             //             start += len;
    //             //         }
    //             //         v.into_pyarray(py).into()
    //             //     } else {
    //             //         let v: Vec<&[u8]> = vec![];
    //             //         let mut start = 0;
    //             //         for len in length {
    //             //             let len = len as usize;
    //             //             v.push(&data[start..start + len]);
    //             //             start += len;
    //             //         }
    //             //         v.into_pyarray(py).into()
    //             //     }
    //             // }
    //             //Column::Blob { data, .. } => data.clone().into_pyarray(py).into(),
    //             _ => present.clone().into_pyarray(py).into(),
    //         };
    //         // Once we're done using it hand off the mask to python
    //         let present = present.into_pyarray(py).into();

    //         let locals = [
    //             ("np", np.into()),
    //             ("a", content),
    //             ("m", present)].into_py_dict(py);
    //         let masked_array = py.eval(
    //             "np.ma.masked_array(a, ~m)", 
    //             None,
    //             Some(&locals)
    //         )?.into();
    //         columns.push((&col_name, masked_array));
    //     }
    //     let df_dict : &PyDict = columns.into_py_dict(py);
    //     let df_dict : PyObject = df_dict.into();
    //     if !as_pandas {
    //         Ok(df_dict)
    //     } else {
    //         // Only import pandas if they request it
    //         let pd = py.import("pandas")?;
    //         let locals = [
    //             ("df_dict", df_dict),
    //             ("pd", pd.into())
    //         ].into_py_dict(py);
    //         let pd_df = py.eval(
    //             "pd.DataFrame(df_dict)", 
    //             None,
    //             Some(&locals)
    //         )?.into();
    //         Ok(pd_df)
    //     }
    // }
}

#[pyclass]
struct PyNullableColumn {
    inner: crate::schemata::NullableColumn,
}
#[pymethods]
impl PyNullableColumn {
    /// Convert a column to a masked numpy array
    ///
    /// This has several advantages and disadvantages against pandas in this context.
    /// * Numpy masked arrays support all data types
    /// * Three different methods are available for retrieving variable length columns
    ///
    /// Accepts
    /// -------
    /// * blob: one of "packed", "padded bytes", "objects", or None
    ///   * blob = "packed":  
    ///     Return blob and string columns as two separate vectors,
    ///     (data, masked_length), where all the content is stored contiguously
    ///     Null blobs are masked, and empty blobs have 0 length.
    ///     This is the native format of ORC so this method is much faster.
    ///   * blob = "padded bytes":  
    ///     All the blobs are padded to the length of the longest blob
    ///     and then stored as a contiguous vector with numpy dtype "|Sn"
    ///     where n is the longest blob.
    ///     Since it creates only one vector, unless your blobs/strings are >50 bytes
    ///     this is probably a good compromise between speed and ease of use
    ///   * blob = "objects":  
    ///     Return the 
    pub fn as_numpy<'py>(&self, py: Python<'py>, blob: Option<&str>) -> PyResult<PyObject> {
        // This function is somewhat involved because we need help from Numpy
        // to generate the masked arrays we want.
        let col = &self.inner;
        // We want to convert the column to an array and mask.
        // The arrays need to be dynamically typed so we cast them to PyObject
        // Then we convert them to masked arrays (inside python)
        let np = py.import("numpy")?;
        let mut suggested_dtype = None;

        let present = col.present_or_trues();
        let content : PyObject = match col.content_or_default() {
            Column::Byte { data, .. } => data.into_pyarray(py).into(),
            Column::Boolean { data, .. } => data.into_pyarray(py).into(),
            Column::Int { data, .. } => data
                .iter()
                .map(|x| *x as i64)
                .collect::<Vec<_>>()
                .into_pyarray(py)
                .into(),
            Column::Float { data, .. } => data.clone().into_pyarray(py).into(),
            Column::Double { data, .. } => data.clone().into_pyarray(py).into(),
            Column::Blob { data, length, utf8} => {
                // Packed vectors are easy
                match blob {
                    Some("packed") => (
                        // Keep the string packed as bytes
                        data.clone().into_pyarray(py).to_object(py),
                        length
                            .iter()
                            .map(|x| *x as i64)
                            .collect::<Vec<_>>()
                            .into_pyarray(py)
                            .to_object(py)
                        ).to_object(py),
                    Some("padded bytes") => {
                        // Pad the vector to get 
                        let mut start = 0;
                        let max_len = length.iter().copied().max().unwrap_or_default() as usize;
                        let mut v: Vec<u8> = vec![0u8; length.len() * max_len];
                        for (row_num, len) in length.iter().enumerate() {
                            let len = *len as usize;
                            v[row_num * max_len .. row_num * max_len + len]
                                .copy_from_slice(&data[start..start + len]);
                            start += len;
                        }
                        // We need Numpy to read this buffer as a vector of packed strings
                        // But if course by default it thinks it's a uint8 array
                        // So we drop into Python to solve that
                        let v = PyBytes::new(py, &v);
                        let locals = [
                            ("np", np.to_object(py)),
                            ("v", v.to_object(py)),
                            ("dtype", format!("|S{}", max_len).to_object(py))
                        ].into_py_dict(py);
                        py.eval(
                            "np.frombuffer(v, dtype=dtype)", 
                            None,
                            Some(&locals)
                        )?.into()
                    },
                    Some("objects") | None => {
                        // Unpack the vector into String objects
                        let mut v: Vec<PyObject> = vec![];
                        let mut start = 0;
                        for len in length {
                            let len = len as usize;
                            if utf8 {
                                v.push(std::str::from_utf8(&data[start..start + len])?.to_object(py));
                            } else {
                                v.push((&data[start..start + len]).to_object(py));
                            }
                            start += len;
                        }
                        suggested_dtype = Some("object");
                        v.to_object(py)
                    },
                    _ => return Err(OrcError::InvalidArgumentException("blob must be one of 'packed', 'padded bytes', 'objects', or None").into())
                }
            }
            _ => {
                return Err(OrcError::SchemaError(format!("Serializing {:?} isn't supported yet", self.inner)).into());
            },
        };
        // Once we're done using it hand off the mask to python
        let present = present.into_pyarray(py).into();

        let locals = [
            ("np", np.into()),
            ("a", content),
            ("m", present),
            ("dtype", suggested_dtype.to_object(py))
        ].into_py_dict(py);
        let masked_array = py.eval(
            "(a[0], np.ma.masked_array(a[1], ~m)) if type(a) is tuple else np.ma.masked_array(a, ~m, dtype=dtype)", 
            None,
            Some(&locals)
        )?;
        Ok(masked_array.into())
    }

    /// Return the column, converted into a pandas series
    pub fn as_pandas<'py>(&self, py: Python<'py>) -> PyResult<PyObject> {
        // Only import pandas if they request it
        let pd = py.import("pandas")?;
        let locals = [
            ("np_form", self.as_numpy(py, None)?),
            ("pd", pd.into())
        ].into_py_dict(py);
        Ok(py.eval(
            "pd.Series(np_form)", 
            None,
            Some(&locals)
        )?.into())
    }
}

#[pymodule]
/// A Python module implemented in Rust.
pub fn orc_format(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyOrcFile>()?;
    m.add_class::<PyStripe>()?;
    m.add_class::<PyNullableColumn>()?;
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
