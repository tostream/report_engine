import io
from re import M, match
from typing import Dict, List
from abc import ABC, abstractmethod
from pandas.core.frame import DataFrame
from pandasql import sqldf
import numpy as np
from domain.remote_storage import sFtpHelper, FtpHelper
from domain.web_api import webapi
from enum import Enum


class ReportConfiguration():
    """setup configuration of report generator.
    
    :param conf: Dictionary of configuration parameters. WebApi connection info
    :type conf: dict
    :param msg: Dictionary of message content from Rabbit MQ
    :type msg: dict
    """
    def __init__(self, conf: Dict, msg: Dict) -> None:
        self.conf = conf
        self._report_id = msg["ReportId"]
        self._export_type = msg["ExportType"]


    @property
    def report_id(self):
        """Report ID.

        """
        return self._report_id

    @property
    def export_type(self):
        """Export Type

        """
        return self._export_type
		

    @property
    def web_api(self):
        """sidetrade python common - Web Api

        """
        api_cfg = self.conf['webapi']
        return wa(api_cfg['url'], api_cfg['user'], api_cfg['password'])
    """"""


class ReportExportType(Enum):
    REMOTE_STORAGE = "shared.remote_storage"


class ReportExport():
    """Prepare the export message back to rabbit MQ
    
    :param export_type: Storage type of the report 
    :type export_type: ReportExportType
    :param export_file_name: the report file name(document path if upload to Object Storage)
    :type export_file_name: str
    """
    def __init__(self,  export_type: ReportExportType, export_file_name: str) -> None:
        self.export_type = export_type
        self.export_file_name = export_file_name

    @property
    def destination_queue(self) -> str:
        """Quene name of export message

        """
        return self.export_type

    @property
    def export_message(self) -> dict:
        """Export message content: Vendor ID and report file

        """
        message =  {
            "ExportFile": self.export_file_name
        }

        if (self.export_type == ReportExportType.REMOTE_STORAGE):
            message["ActionType"] = "Upload"

        return message


class Report(ABC):
    @property
    @abstractmethod
    def report_id(self) -> str:
        """Implement the report id for the report

        """
        raise NotImplementedError

    @abstractmethod
    def generate(logger, configuration: ReportConfiguration) -> ReportExport:
        """To implement the Actual report logic

        """
        raise NotImplementedError

    def upload_report(self, configuration: ReportConfiguration, file_name: str, file: io.BytesIO, folder_path: str = None):
        """to store the report once exported from report script.

        :param configuration: report config.
        :type configuration: ReportConfiguration
        :param file_name: name of the exported file
        :type file_name: str
        :param file: the file content 
        :type file: io.BytesIO
        :param folder_path: target folder 
        :type folder_path: str

        """
		if configuration.export_type == "FTP":
            ftp_config = configuration.conf['FtpConnectionId']
            upload = FtpHelper(ftp_config)
            if not folder_path is None:
                upload.set_directory(folder_path)
            upload.put_file(file_name, file)
            return file_name
        elif configuration.export_type == "SFTP":
            sftp_config = configuration.conf['FtpConnectionId']
            upload = sFtpHelper(sftp_config)
            if not folder_path is None:
                upload.set_directory(folder_path)
            upload.put_file(file_name, file)
            return file_name

    def column_to_cte(self, df: DataFrame, column: str, cte_name: str) -> str:
        ids = np.unique(df[column].to_numpy())
        id_selects: List[str] = list()

        for id_index, id_value in enumerate(ids):
            if id_index == 0:
                id_selects.append(f'SELECT \'{id_value}\' as id FROM dual\n')
            else:
                id_selects.append(f'UNION ALL\n')
                id_selects.append(f'SELECT \'{id_value}\' as id FROM dual\n')

        cte_ids = ''.join(id_selects)

        return (
            f'WITH {cte_name}\n'
            f'AS\n'
            f'('
            f'{cte_ids}'
            f')\n')
        
    def pysqldf(self, q):
        """To build Pandas DataFrame by Pandas SQL

        :param q: query statment.
        :type q: str
        """
        return sqldf(q, globals())
		
    def convert_column(self,df:DataFrame,search_str:str,replace_str:str,regex_flag:bool = False):
        """_summary_
        globally replace a string to the pandas data frame

        Args:
            self (_type_): _description_
            df (DataFrame): _description_
            search_str (str): _description_
            replace_str (str): _description_
            regex_flag (bool, optional): _description_. Defaults to False.

        Returns:
            _type_: _description_
        """
        df = df.replace(search_str,replace_str, regex=regex_flag) 
        return df

    def fix_column_length(self,df:DataFrame,column_name:str,col_len:int):
        """_summary_
        to fix the length of a column in the pandas data frame

        Args:
            self (_type_): _description_
            df (DataFrame): _description_
            column_name (str): _description_
            col_len (int): _description_

        Returns:
            _type_: _description_
        """
        df[column_name] = df[column_name].str[:int(col_len)]
        return df
