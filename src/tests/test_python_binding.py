from orc_format import PyOrcFile
from pathlib import Path
import numpy as np

def test_read_tiny_file():
    sample_orc_path = Path(__file__).parent.joinpath("sample.orc")
    print(sample_orc_path.stat())
    toc = PyOrcFile(filename=sample_orc_path.as_posix())
    df = toc.stripe(0).dataframe(toc)
    assert df == [
        np.array([-1111.], dtype=np.float32),
        np.array([ 0.,  8., 20.,  0.,  0.,  0.,  0.,  0.], dtype=np.float32),
        np.array([ 0.,  8., 20.,  0.,  0.,  0.,  0.,  0.], dtype=np.float32),
        np.array([ 0.,  8., 20.,  0.,  0.,  0.,  0.,  0.], dtype=np.float32),
        np.array([ 0.,  8., 20.,  0.,  0.,  0.,  0.,  0.], dtype=np.float32),
        np.array([ 0.,  8., 20.,  0.,  0.,  0.,  0.,  0.], dtype=np.float32),
        np.array([ 0.,  8., 20.,  0.,  0.,  0.,  0.,  0.], dtype=np.float32),
        np.array([-1111.], dtype=np.float32),
        np.array([-1111.], dtype=np.float32),
        np.array([-1111.], dtype=np.float32),
        np.array([-1111.], dtype=np.float32),
        np.array([-1111.], dtype=np.float32),
        np.array([-1111.], dtype=np.float32),
        np.array([-1111.], dtype=np.float32),
        np.array([      0., -350345., -350345.,       0.,       0.,       0.,             0.,       0.], dtype=np.float32),
        np.array([-1111.], dtype=np.float32),
        np.array([-1111.], dtype=np.float32),
        np.array([-1111.], dtype=np.float32),
        np.array([-1111.], dtype=np.float32),
        np.array([-1111.], dtype=np.float32),
        np.array([-1111.], dtype=np.float32),
        np.array([-1111.], dtype=np.float32),
        np.array([-1111.], dtype=np.float32),
        np.array([-1111.], dtype=np.float32)
    ]