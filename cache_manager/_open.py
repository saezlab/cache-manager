from __future__ import annotations

import re
import os
import gzip
import tarfile
import zipfile
import io
import struct

from pypath_common import _constants as _const
from pypath_common import _misc as _common

from cache_manager._session import _log

COMPRESSED =  {'gz', 'xz', 'bz2'}
ARCHIVES = {'zip', 'tar.gz', 'tar.bz2', 'tar.xz'}



class FileOpener:
    """
    This class opens a file, extracts it in case it is a
    gzip, tar.gz, tar.bz2 or zip archive, selects the requested
    files if you only need certain files from a multifile archive,
    reads the data from the file, or returns the file pointer,
    as you request. It examines the file type and size.
    """

    FORBIDDEN_CHARS = re.compile(r'[/\\<>:"\?\*\|]')

    def __init__(
            self,
            path: str,
            ext: str | None = None,
            needed: list[str] | None = None,
            large: bool = True,
            default_mode: str = 'r',
            encoding: str | None = None,
        ):

        for k, v in locals().items():

            if k == "self":
                continue

            setattr(self, k, v)



    def __del__(self):
        self.close()


    def open(self):
        """
        Opens the file if exists.
        """

        if not os.path.exists(self.path):

            msg = f'No such file: `{self.path}`.'
            _log(msg)
            raise FileNotFoundError(msg)

        mode, encoding = (
            (self.default_mode, self.encoding)
                if self.type == 'plain' else
            ('rb', None)
        )
        self.fileobj = open(self.path, mode = mode, encoding = encoding)


    def close(self):
        """
        Close the file.
        """

        if hasattr(self, "fileobj") and hasattr(self.fileobj, "close"):
            self.fileobj.close()


    def extract(self):
        """
        Calls the extracting method for compressed files.
        """

        getattr(self, 'open_%s' % self.type)()


    def open_tar(self):
        """
        Extracts files from tar.
        """

        _log(f'Opening tar file: {self.path}')

        self.files = {}
        self.sizes = {}
        compr = self.ext.split('.')[-1]
        self.tarfile = tarfile.open(fileobj = self.fileobj, mode = f'r:{compr}')
        self.members = self.tarfile.getmembers()

        for m in self.members:

            if (
                (
                    self.needed is None or
                    m.name in self.needed
                )
                # m.size is 0 for dierctories
                and m.size != 0
            ):

                this_file = self.tarfile.extractfile(m)
                self.sizes[m.name] = m.size

                if self.large:

                    self.files[m.name] = this_file

                else:
                    _log(f'Reading contents of file from archive: `{m.name}`.')
                    self.files[m.name] = this_file.read()
                    this_file.close()

        if not self.large:

            self.tarfile.close()
            self._log(f'File closed: `{self.path}`.')

        self.result = self.files


    def open_gz(self):

        _log(f'Opening gzip file: {self.path}')

        self.fileobj.seek(-4, 2)
        self.size = struct.unpack('I', self.fileobj.read(4))[0]
        self.fileobj.seek(0)
        self.gzfile = gzip.GzipFile(fileobj = self.fileobj)

        if self.large:

            io.DEFAULT_BUFFER_SIZE = 4096
            self._gzfile_mode_r = io.TextIOWrapper(
                self.gzfile,
                encoding = self.encoding,
            )
            self.result = self.iterfile(
                self.gzfile
                    if self.default_mode == 'rb' else
                self._gzfile_mode_r
            )
            _log(f'Result is an iterator over the lines of {self.path}')

        else:

            self.result = self.gzfile.read()
            self.gzfile.close()
            _log(f'Data has been read from gzip file {self.path}. The file has been closed.')


    def open_zip(self):

        _log(f'Opening zip file {self.path}')

        self.files_multipart = {}
        self.sizes = {}
        self.fileobj.seek(0)
        self.zipfile = zipfile.ZipFile(self.fileobj, 'r')
        self.members = self.zipfile.namelist()

        for i, m in enumerate(self.members):

            self.sizes[m] = self.zipfile.filelist[i].file_size

            if self.needed is None or m in self.needed:

                this_file = self.zipfile.open(m)

                if self.large:

                    if self.default_mode == 'rb':

                        # keeping it in binary mode
                        self.files_multipart[m] = this_file

                    else:

                        # wrapping the file for decoding
                        self.files_multipart[m] = io.TextIOWrapper(
                            this_file, encoding=self.encoding
                        )
                else:

                    self.files_multipart[m] = this_file.read()
                    this_file.close()

        if not self.large:

            self.zipfile.close()
            _log(f'Data has been read from zip file {self.path}. File has been closed')

        self.result = self.files_multipart


    def open_plain(self):

        _log(f'Opening plain text file {self.path}')

        self.size = os.path.getsize(self.fileobj.name)

        if self.large:

            self.result = self.iterfile(self.fileobj)

        else:

            self.result = self.fileobj.read()
            self.fileobj.close()
            _log(f'Contents of {self.path} has been read and the file has been closed.')

    def set_type(self):

        ext = self.ext or _common.ext(self.path)
        ext = ext.strip(".")
        ext = 'tar.gz' if ext == 'tgz' else ext

        self.type = ext if ext in COMPRESSED | ARCHIVES else 'plain'
        self.type = 'tar' if self.type.startswith('tar') else self.type


    @staticmethod
    def iterfile(fileobj):

        for line in fileobj:

            yield line
