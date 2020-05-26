
class EngineBase(object):
    """引擎基类"""
    def __init__(self, host, username, passwd,pasv):
        self.link = self.connect(host, username, passwd,pasv)

    def connect(self, host, username, passwd,pasv):
        """连接"""
        pass

    def downLoad(self, dataName, callback):
        """从网络获取文件的基类"""
        pass

    def getFileList(self):
        """获取数据列表"""
        pass

    def close(self):
        """关闭连接"""
        pass
