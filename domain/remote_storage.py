
from ftplib import FTP
import pysftp
import abc
import io
import logging as log

class AbstractRemoteStorage(abc.ABC):

    def __init__(self, conf) -> None:
        self.conn = self.connect(conf)

    @abc.abstractmethod
    def get_file(self, filepath):
        raise NotImplementedError

    @abc.abstractmethod
    def put_file(self, filepath, file):
        raise NotImplementedError

    @abc.abstractmethod
    def get_file_list(self, file_prefix) -> list:
        raise NotImplementedError

    @abc.abstractmethod
    def remote_conn(self, conf):
        raise NotImplementedError

    @abc.abstractmethod
    def set_directory(self, remotepath):
        raise NotImplementedError

    @abc.abstractmethod
    def get_directory(self):
        raise NotImplementedError

    def connect(self, conf):
        log.info("connecting to remote storage ")
        try:
            return self.remote_conn(conf)
        except IOError as e:
            log.error(f"remote storage connection refused: {e} ")
            raise (f"remote storage connection refused: {e}")

    def close(self, remote_storage):
        self.coon.close()


class FtpHelper(AbstractRemoteStorage):
    """[summary]

    Args:
        AbstractRemoteStorage ([type]): [description]
    """
    def __init__(self, conf) -> None:
        super().__init__(conf)

    def get_file(self, filepath):
        destfile = io.BytesIO()
        self.conn.retrbinary('RETR ' + filepath, destfile.write, 1024)
        destfile.seek(0)
        return destfile

    def put_file(self, filepath, file):
        ftpCommand = f"STOR {filepath}"
        ftpresponse = self.conn.storbinary(ftpCommand, fp=file)
        return ftpresponse

    def delete_file(self, filepath):
        self.conn.delete(filepath)

    def get_file_list(self, file_prefix='*') -> list:
        return list(filter(lambda x: x.startswith(file_prefix), self.conn.nlst()))

    def set_directory(self, remotepath):
        self.conn.cwd(remotepath)

    def get_directory(self):
        return self.conn.pwd()

    def remote_conn(self, conf) -> FTP:
        """[summary]

        Args:
            conf ([type]): [expecting connection info from WebApi] 

        Returns:
            FTP: [description]
        """
        return FTP(host=conf['Host'], user=conf['User'], passwd=conf['Password'])


class sFtpHelper(AbstractRemoteStorage):

    def __init__(self, conf) -> None:
        super().__init__(conf)

    def get_file(self, filepath):
        destfile = io.BytesIO()
        self.conn.getfo(filepath, destfile)
        destfile.seek(0)
        return destfile

    def put_file(self, filepath, file):
        return self.conn.putfo(file, filepath)

    def get_file_list(self, file_prefix) -> list:
        return list(filter(lambda x: x.startswith(file_prefix), self.conn.listdir()))

    def delete_file(self, filepath):
        self.conn.remove(filepath)

    def set_directory(self, remotepath):
        self.conn.cwd(remotepath)

    def get_directory(self):
        return self.conn.pwd()

    def remote_conn(self, conf) -> pysftp:
        """[summary]

        Args:
            conf ([type]): [[expecting connection info from WebApi]

        Returns:
            pysftp: [description]
        """
        try:
            cnopts = pysftp.CnOpts()
            cnopts.hostkeys = None
            return pysftp.Connection(host=conf['Host'], username=conf['User']
                    , password=conf['Password']
                    , port=conf['Port']
                    , default_path=conf['SourceDirectory']
                    , cnopts=cnopts)
        except Exception as e:
            raise (e)
