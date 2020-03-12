MONGO_URL = 'localhost'
MONGO_DB = 'meituanComment'
MONGO_TABLE = 'comment'

# 配置DB
DATABASES = {
    "default": {
        "TYPE": "SSDB",  # 目前支持SSDB或redis数据库
        "HOST": "127.0.0.1",  # db host
        "PORT": 8888,  # db port，例如SSDB通常使用8888，redis通常默认使用6379
        "NAME": "proxy",  # 默认配置
        "PASSWORD": ""  # db password

    }
}

# 配置 ProxyGetter

PROXY_GETTER = [
    "freeProxy01",  # 这里是启用的代理抓取函数名，可在ProxyGetter/getFreeProxy.py 扩展
    "freeProxy02"
]

# 配置 API服务

SERVER_API = {
    "HOST": "0.0.0.0",  # 监听ip, 0.0.0.0 监听所有IP
    "PORT": 5010  # 监听端口
}

# 上面配置启动后，代理池访问地址为 http://127.0.0.1:5010