import pytest
import os
import gzip
import zipfile
import tarfile

from cache_manager import utils
from cache_manager import _open


@pytest.fixture
def temp_tar_gz_file(tmpdir):
    subfolder = tmpdir.mkdir("subfolder")
    file1 = subfolder.join("file1.txt")
    file2 = subfolder.join("file2.txt")
    file1.write("This is the first file.")
    file2.write("This is the second file.")

    gz_path = tmpdir.join("archive.tar.gz")

    with tarfile.open(gz_path, "w:gz") as tar:
        tar.add(subfolder.strpath, arcname=os.path.basename(subfolder.strpath))

    return gz_path

@pytest.fixture
def temp_xz_file(tmpdir):
    subfolder = tmpdir.mkdir("subfolder")
    file1 = subfolder.join("file1.txt")
    file2 = subfolder.join("file2.txt")
    file1.write("This is the first file.")
    file2.write("This is the second file.")

    xz_path = tmpdir.join("archive.tar.xz")

    with tarfile.open(xz_path, "w:xz") as tar:
        tar.add(subfolder.strpath, arcname=os.path.basename(subfolder.strpath))
    
    return xz_path

@pytest.fixture
def temp_bz2_file(tmpdir):
    subfolder = tmpdir.mkdir("subfolder")
    file1 = subfolder.join("file1.txt")
    file2 = subfolder.join("file2.txt")
    file1.write("This is the first file.")
    file2.write("This is the second file.")

    bz2_path = tmpdir.join("archive.tar.bz2")

    with tarfile.open(bz2_path, "w:bz2") as tar:
        tar.add(subfolder.strpath, arcname=os.path.basename(subfolder.strpath))

    return bz2_path

@pytest.fixture
def temp_zip_file(tmpdir):
    subfolder = tmpdir.mkdir("subfolder")
    file1 = subfolder.join("file1.txt")
    file2 = subfolder.join("file2.txt")
    file1.write("This is the first file.")
    file2.write("This is the second file.")

    zip_path = tmpdir.join("archive.zip")

    with zipfile.ZipFile(zip_path, 'w') as zipf:
        zipf.write(file1.strpath, os.path.join("subfolder", "file1.txt"))
        zipf.write(file2.strpath, os.path.join("subfolder", "file2.txt"))

    return zip_path

@pytest.fixture
def temp_gz_file(tmpdir):
    gz_path = tmpdir.join("archive.gz")

    with gzip.open(gz_path, "w") as gz:
        gz.write(b"This is the first file.")

    return gz_path

def test_gz_file(temp_gz_file):
    assert temp_gz_file.check(file=1)

def test_xz_file(temp_xz_file):
    assert temp_xz_file.check(file=1)

def test_bz2_file(temp_bz2_file):
    assert temp_bz2_file.check(file=1)

def test_zip_file(temp_zip_file):
    assert temp_zip_file.check(file=1)

def test_tar_gz_file(temp_tar_gz_file):
    assert temp_tar_gz_file.check(file=1)

