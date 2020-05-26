# coding=utf-8
import warnings
warnings.filterwarnings("ignore")
import os
import socket
import ftplib
from common.transport.enginebase import EngineBase
class ftp_engine(EngineBase):
    """FTP数据引擎"""
    def __init__(self, host, username, passwd, pasv=True):
        EngineBase.__init__(self, host, username, passwd,pasv)

    def connect(self, host, username, passwd, pasv=True):
        """连接"""
        try:
            link = ftplib.FTP(host)
        except (socket.error, socket.gaierror) as e:
            print('Error, cannot reach ' + host)
            return
        else:
            print('Connect To Host Success...')

        try:
            link.login(username, passwd)
            link.set_pasv(pasv)
        except ftplib.error_perm:
            print('Username or Passwd Error')
            link.quit()
            return
        else:
            print('Login Success...')

        return link

    def downLoad(self, dataName, callback):
        """Ftp文件下载，callback为回调函数"""
        try:
            print(dataName, 'Downloading')
            self.link.retrbinary(f'RETR {dataName}', callback)
        except ftplib.error_perm as e:
            print(e, 'File Error')
            #os.unlink(localpath)
        else:
            print(dataName, 'Download Success...')

    def ftpUpload(self, remotepath, localpath):
        """上传数据"""
        try:
            self.link.storbinary('STOR %s' % remotepath, open(localpath, 'rb'))
        except ftplib.error_perm:
            print('File Error')
            os.unlink(localpath)
        else:
            print('Upload Success...')

    def getFileList(self):
        """获得数据列表"""
        return self.link.nlst()

    def close(self):
        self.link.quit()

