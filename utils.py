# -*- coding:utf-8 -*-

import re

with open("utils", "rb+") as f:  # 打开文件
    plate = f.readline().decode()
    print(plate)
    result = re.findall(u"[\u4e00-\u9fa5]+", plate)
    results = []
    for i in result:
        if i not in results:
            results.append(i)
    print(results)

