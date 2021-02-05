import json
import sys
import urllib
import requests


def report(count, blockSize, totalSize):
    percent = int(count*blockSize*100/totalSize)
    sys.stdout.write("\r%d%%" % percent + ' complete')
    sys.stdout.flush()


headers = {'Accept': '*/*',
           'Accept-Encoding': 'gzip, deflate',
           'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
           'Host': 'query.sse.com.cn',
           'Referer': 'http://kcb.sse.com.cn/',
           }
url = 'http://query.sse.com.cn/commonSoaQuery.do?&isPagination=true&sqlId=GP_GPZCZ_SHXXPL&fileVersion=1&pageHelp.pageSize=1000&fileType=30&pageHelp.pageNo=1'
response = requests.get(url, headers=headers)
results = json.loads(response.text)['result']

print("Start working")
file_path = "D:/"
print(file_path)
count = 0
for i in results:
    count += 1
    if count == 456:
        pdf_url = 'http://static.sse.com.cn/stock' + i.get('filePath')
        file_name = i.get('filePath')[22:]
        print("\n" + "Downloading " + str(count) + " file")
        urllib.request.urlretrieve(pdf_url, filename=file_path + file_name, reporthook=report)


