import requests
from data_engine import setting

class DevCloudWrapper:
    _session_id = None
    _token = None
    _project_id = '4d5e9315632c47288a151cdd78359ae4'
    _project_name = 'cn-east-2'
    _wiki_write_url = 'https://hd.devcloud.huaweicloud.com/wiki/v1/wiki/createOrEditDocInfo'
    _sso_url = 'https://auth.huaweicloud.com/authui/validateUser.action?service=https://devcloudsso.huaweicloud.com'\
                  '/authticket?service=https://hd.devcloud.huaweicloud.com/'

    @classmethod
    def write_document(cls, title, content):
        #TODO token过期后再获取
        cls._session_id, cls._token = cls.get_token()
        cookies = {'devclouddevuish420J_SESSION_ID': cls._session_id}
        body = {'projectId': cls._project_id,
                'title': title,
                'cid': [],
                'summary': '',
                'ismarkdown': 0,
                'paragraphList': [],
                'content': content,
                'createType': 1}
        headers = {
            'Content-Type': 'application/json',
            'projectname': cls._project_name,
            'cftk': cls._token
        }
        print('正在写入 %s' % title)
        requests.post(url=cls._wiki_write_url, headers=headers, json=body, cookies=cookies)

    @classmethod
    def get_token(cls):
        try:
            s = requests.session()
            s.headers['User-Agent'] = 'Mozilla/5.0'
            data = {'userpasswordcredentials.username': setting.DEVCLOUD_USER,
                    'userpasswordcredentials.password': setting.DEVCLOUD_PWD,
                    'userpasswordcredentials.domain': '',
                    'userpasswordcredentials.domainType': 'name',
                    'userpasswordcredentials.countryCode': '0086',
                    'userpasswordcredentials.verifycode': '',
                    'userpasswordcredentials.companyLogin': False,
                    'userpasswordcredentials.userInfoType': 'name',
                    '__checkbox_warnCheck': True,
                    'isAjax': True,
                    'Submit': 'Login'}
            sso_auth_res = requests.post(cls._sso_url, data=data)
            sso_data = sso_auth_res.json()
            redirect_url = sso_data['redirectUrl'].replace(' ', '')
            redirect_res = s.get(url=redirect_url, allow_redirects=False)
            redirect_url2 = redirect_res.headers['Location']
            s.get(url=redirect_url2, allow_redirects=False)
            cookie_dict = s.cookies.get_dict()
            if 'devclouddevuish420J_SESSION_ID' in cookie_dict and 'devclouddevuish420cftk' in cookie_dict:
                print('成功获取华为云token')
            else:
                raise RuntimeError('获取华为云token失败')
            return cookie_dict['devclouddevuish420J_SESSION_ID'], cookie_dict['devclouddevuish420cftk']
        except Exception as e:
            raise RuntimeError('未知错误: %s' % repr(e))


