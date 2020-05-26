#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020/1/15 11:32
# @Author  : jwliu
# @Site    : 
# @Software: PyCharm

import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

class mail(object):
    def __init__(self,host,user,password,port=25):
        self._smtpobj = smtplib.SMTP(host=host,port=port)
        self._smtpobj.login(user=user,password=password)
        self._from = user

    def __del__(self):
        try:
            self._smtpobj.quit()
        except Exception as e:
            print(e)

    def send_html(self,Subject,mail_to_list,body_html,filename=None):
        msg = MIMEMultipart()  # 给定msg类型
        msg['Subject'] = Subject
        msg['From'] = self._from
        msg['To'] = ";".join(mail_to_list)
        html_sub = MIMEText(body_html, 'html', 'utf-8')

        if filename is None:
            pass
        elif isinstance(filename,str):
            filenames = os.path.split(filename)
            if len(filenames)>1:
                fn = filenames[1]
            else:
                fn = filenames[0]
            att1 = MIMEText(open(filename, 'rb').read(), 'xls', 'gb2312')
            att1["Content-Type"] = 'application/octet-stream'
            att1["Content-Disposition"] = 'attachment;filename=' + fn
            msg.attach(att1)
        elif isinstance(filename,list):
            for each in filename:
                filenames = os.path.split(each)
                if len(filenames)>1:
                    fn = filenames[1]
                else:
                    fn = filenames[0]
                att1 = MIMEText(open(each, 'rb').read(), 'xls', 'gb2312')
                att1["Content-Type"] = 'application/octet-stream'
                att1["Content-Disposition"] = 'attachment;filename=' + fn
                msg.attach(att1)

        msg.attach(html_sub)
        self._smtpobj.sendmail(self._from , mail_to_list, msg.as_string())
        print('send mail',Subject)

if __name__ == '__main__':

    url = "https://blog.csdn.net/chinesepython"
    html_info = """
        <p>点击以下链接，你会去向一个更大的世界</p>
        <p><a href="%s">click me</a></p>
        <p>i am very galsses for you</p>
        """ % url

    ml = mail(host='smtp.mxhichina.com',user='jwliu@jzassetmgmt.com',password='password*',port=25)
    ml.send_html(Subject='test mail by python',mail_to_list=['49680664@qq.com','jwliu@jzassetmgmt.com'],body_html=html_info)