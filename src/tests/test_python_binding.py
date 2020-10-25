from orc_format import PyOrcFile
from pathlib import Path
import numpy as np
import pandas as pd

def test_read_tiny_file():
    ma = np.ma.masked_array
    sample_orc_path = Path(__file__).parent.joinpath("sample.orc")
    toc = PyOrcFile(filename=sample_orc_path.as_posix())
    assert toc.stripe_count() == 1
    stripe = toc.stripe(0)
    assert stripe.cols() == 24

    # First column: boolean
    # TODO: Fails with "Reached end of buffer while reading IntRLE1 missing run delta"
    #column = stripe.column(0, toc)
    #assert (column.as_numpy() == ma([False, True, True], [True, False, False])).all()
    # Remember pandas doesn't have null so the first element is sorta undefined
    #assert (column.as_pandas() == pd.Series([True, True, True])).all()


    # First column: boolean
    # TODO: Fails with "Reached end of buffer while reading IntRLE1 missing run delta"
    column = stripe.column(1, toc)
    assert column.as_numpy().dtype == np.bool_
    assert (column.as_numpy() == ma([False, True, True], [True, False, False])).all()
    assert (
        (column.as_pandas() == pd.Series([np.nan, True, True]))
        == pd.Series([False, True, True]) # Because nan != nan
    ).all()
    
    # Second column: a byte
    column = stripe.column(2, toc)
    assert column.as_numpy().dtype == np.uint8
    assert (column.as_numpy() == ma([0, 8, 20], [True, False, False])).all()
    assert (
        (column.as_pandas() == pd.Series([np.nan, 8., 20.]))
        == pd.Series([False, True, True]) # Because nan != nan
    ).all()

    # Third column: int8 (but for now we return int64)
    # TODO: Integer types
    column = stripe.column(3, toc)
    assert column.as_numpy().dtype == np.int64
    assert (column.as_numpy() == ma([False, 8, 20], [True, False, False])).all()
    assert (
        (column.as_pandas() == pd.Series([np.nan, 8., 20.]))
        == pd.Series([False, True, True]) # Because nan != nan
    ).all()

    # Fourth column: int16 (but for now we return int64)
    # TODO: Integer types
    column = stripe.column(4, toc)
    assert column.as_numpy().dtype == np.int64
    assert (column.as_numpy() == ma([False, 8, 20], [True, False, False])).all()
    assert (
        (column.as_pandas() == pd.Series([np.nan, 8., 20.]))
        == pd.Series([False, True, True]) # Because nan != nan
    ).all()

    # Fifth column: int32 (but for now we return int64)
    # TODO: Integer types
    column = stripe.column(5, toc)
    assert column.as_numpy().dtype == np.int64
    assert (column.as_numpy() == ma([False, 8, 20], [True, False, False])).all()
    assert (
        (column.as_pandas() == pd.Series([np.nan, 8., 20.]))
        == pd.Series([False, True, True]) # Because nan != nan
    ).all()

    # Sixth column: float32
    column = stripe.column(6, toc)
    assert column.as_numpy().dtype == np.float32
    assert (column.as_numpy() == ma([False, 8., 20.], [True, False, False])).all()
    assert (
        (column.as_pandas() == pd.Series([np.nan, 8., 20.]))
        == pd.Series([False, True, True]) # Because nan != nan
    ).all()

    # Seventh column: float64
    column = stripe.column(7, toc)
    assert column.as_numpy().dtype == np.float64
    assert (column.as_numpy() == ma([False, 8., 20.], [True, False, False])).all()
    assert (
        (column.as_pandas() == pd.Series([np.nan, 8., 20.]))
        == pd.Series([False, True, True]) # Because nan != nan
    ).all()

    # Eighth column: what?
    # column = stripe.column(8, toc)
    # assert column.as_numpy().dtype == np.float64
    # assert (column.as_numpy() == ma([False, 8, 20], [True, False, False])).all()
    # assert (
    #     (column.as_pandas() == pd.Series([np.nan, 8., 20.]))
    #     == pd.Series([False, True, True]) # Because nan != nan
    # ).all()

    # Ninth column: Char
    column = stripe.column(9, toc)
    assert column.as_numpy().dtype == np.dtype("O")
    assert (column.as_numpy() == ma([False, "8", "2"], [True, False, False])).all()
    assert (
        (column.as_pandas() == pd.Series([np.nan, "8", "2"]))
        == pd.Series([False, True, True]) # Because nan != nan
    ).all()

    # Tenth column: Char(3)
    # There are three ways to send these, depending on your needs.
    column = stripe.column(10, toc)

    # Packed representation: the most efficient and least convenient.
    # It returns a data array and a masked length array
    data, lengths = column.as_numpy("packed")
    assert data.dtype == np.dtype(np.uint8)
    assert lengths.dtype == np.dtype(np.int64)
    assert np.all(lengths.mask == [True, False, False])
    assert np.all(lengths == [0, 3, 3])

    # Padded bytes: a compromise between speed and efficiency
    packed_bytes = column.as_numpy("padded bytes")
    assert packed_bytes.dtype == "|S3"
    assert np.all(packed_bytes.mask == [True, False, False])
    assert np.all(packed_bytes == [b"", b"8  ", b"20 "])

    # Objects: Not fast but easy to use
    objects = column.as_numpy("objects")
    assert np.all(objects.mask == [True, False, False])
    assert np.all(objects == ["", "8  ", "20 "])

    # Pandas series in this case are always objects
    assert (
        (column.as_pandas() == pd.Series([None, "8  ", "20 "]))
        == pd.Series([False, True, True]) # Because nan != nan
    ).all()

    # assert np.all(df == [
    #     ma([False, True, True], [True, False, False]),
    #     ma([ 0,  8, 20], [True, False, False]),
    #     np.array([ 0.,  8., 20.], dtype=np.float32),
    #     np.array([ 0.,  8., 20.], dtype=np.float32),
    #     np.array([ 0.,  8., 20.], dtype=np.float32),
    #     np.array([ 0.,  8., 20.], dtype=np.float32),
    #     np.array([ 0.,  8., 20.], dtype=np.float32),
    #     np.array([-1111.], dtype=np.float32),
    #     np.array([-1111.], dtype=np.float32),
    #     np.array([-1111.], dtype=np.float32),
    #     np.array([-1111.], dtype=np.float32),
    #     np.array([-1111.], dtype=np.float32),
    #     np.array([-1111.], dtype=np.float32),
    #     np.array([-1111.], dtype=np.float32),
    #     np.array([      0., -350345., -350345.,       0.,       0.,       0.,             0.,       0.], dtype=np.float32),
    #     np.array([-1111.], dtype=np.float32),
    #     np.array([-1111.], dtype=np.float32),
    #     np.array([-1111.], dtype=np.float32),
    #     np.array([-1111.], dtype=np.float32),
    #     np.array([-1111.], dtype=np.float32),
    #     np.array([-1111.], dtype=np.float32),
    #     np.array([-1111.], dtype=np.float32),
    #     np.array([-1111.], dtype=np.float32),
    #     np.array([-1111.], dtype=np.float32)
    # ])