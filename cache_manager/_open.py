import re
import os
import gzip
import tarfile
import zipfile
import io
import struct

from pypath_common import _constants as _const

from cache_manager._session import _log

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
            file_param,
            compr = None,
            extract = True,
            _open = True,
            set_fileobj = True,
            files_needed = None,
            large = True,
            default_mode = 'r',
            encoding = 'utf-8',
        ):

        if not hasattr(self, 'encoding') or not self.encoding:

            self.encoding = encoding

        if not hasattr(self, 'default_mode'):

            self.default_mode = default_mode

        if not hasattr(self, 'compr'):
            self.compr = compr
        if not hasattr(self, 'files_needed'):
            self.files_needed = files_needed
        if not hasattr(self, 'large'):
            self.large = large
        self.fname = file_param \
            if type(file_param) in _const.CHAR_TYPES else file_param.name
        self.fileobj = None \
            if type(file_param) in _const.CHAR_TYPES else file_param
        if not hasattr(self, 'type'):
            self.get_type()
        if _open:
            self.open()
        if extract:
            self.extract()


    def open(self):
        """
        Opens the file if exists.
        """

        if self.fileobj is None and os.path.exists(self.fname):

            if self.encoding and self.type == 'plain':

                self.fileobj = open(
                    self.fname,
                    self.default_mode,
                    encoding = (
                        None if self.default_mode == 'rb' else self.encoding
                    ),
                )

            else:

                self.fileobj = open(self.fname, 'rb')


    def extract(self):
        """
        Calls the extracting method for compressed files.
        """

        getattr(self, 'open_%s' % self.type)()


    def open_tgz(self):
        """
        Extracts files from tar gz.
        """

        self._log('Opening tar.gz file `%s`.' % self.fileobj.name)

        self.files_multipart = {}
        self.sizes = {}
        self.tarfile = tarfile.open(fileobj = self.fileobj, mode = 'r:gz')
        self.members = self.tarfile.getmembers()

        for m in self.members:

            if (self.files_needed is None or m.name in self.files_needed) \
                    and m.size != 0:
                # m.size is 0 for dierctories
                this_file = self.tarfile.extractfile(m)
                self.sizes[m.name] = m.size
                if self.large:
                    self.files_multipart[m.name] = this_file
                else:
                    self._log(
                        'Reading contents of file '
                        'from archive: `%s`.' % m.name
                    )
                    self.files_multipart[m.name] = this_file.read()
                    this_file.close()

        if not self.large:
            self.tarfile.close()
            self._log('File closed: `%s`.' % self.fileobj.name)

        self.result = self.files_multipart


    def open_gz(self):

        self._log('Opening gzip file `%s`.' % self.fileobj.name)

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
            self._log(
                'Result is an iterator over the '
                'lines of `%s`.' % self.fileobj.name
            )

        else:

            self.result = self.gzfile.read()
            self.gzfile.close()
            self._log(
                'Data has been read from gzip file `%s`. '
                'The file has been closed' % self.fileobj.name
            )


    def open_zip(self):

        self._log('Opening zip file `%s`.' % self.fileobj.name)

        self.files_multipart = {}
        self.sizes = {}
        self.fileobj.seek(0)
        self.zipfile = zipfile.ZipFile(self.fileobj, 'r')
        self.members = self.zipfile.namelist()
        for i, m in enumerate(self.members):
            self.sizes[m] = self.zipfile.filelist[i].file_size
            if self.files_needed is None or m in self.files_needed:
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
            self._log(
                'Data has been read from zip file `%s`.'
                'File has been closed' % self.fileobj.name
            )

        self.result = self.files_multipart

    def open_plain(self):

        self._log('Opening plain text file `%s`.' % self.fileobj.name)

        self.size = os.path.getsize(self.fileobj.name)

        if self.large:

            self.result = self.iterfile(self.fileobj)

        else:

            self.result = self.fileobj.read()
            self.fileobj.close()
            self._log(
                'Contents of `%s` has been read '
                'and the file has been closed.' % self.fileobj.name
            )

    def get_type(self):

        self.multifile = False
        if self.fname[-3:].lower() == 'zip' or self.compr == 'zip':
            self.type = 'zip'
            self.multifile = True
        elif self.fname[-3:].lower() == 'tgz' or \
                self.fname[-6:].lower() == 'tar.gz' or \
                self.compr == 'tgz' or self.compr == 'tar.gz':
            self.type = 'tgz'
            self.multifile = True
        elif self.fname[-2:].lower() == 'gz' or self.compr == 'gz':
            self.type = 'gz'
        else:
            self.type = 'plain'


    @staticmethod
    def iterfile(fileobj):

        for line in fileobj:

            yield line


