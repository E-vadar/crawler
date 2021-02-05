import datetime
from datetime import timedelta
import pandas as pd
import requests
import random
import time
import os

User_Agent = [
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)',
    'Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 1.0.3705; .NET CLR 1.1.4322)',
    'Mozilla/4.0 (compatible; MSIE 7.0b; Windows NT 5.2; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.2; .NET CLR 3.0.04506.30)',
    'Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN) AppleWebKit/523.15 (KHTML, like Gecko, Safari/419.3) Arora/0.3 (Change: 287 c9dfb30)',
    'Mozilla/5.0 (X11; U; Linux; en-US) AppleWebKit/527+ (KHTML, like Gecko, Safari/419.3) Arora/0.6',
    'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.2pre) Gecko/20070215 K-Ninja/2.1.1',
    'Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9) Gecko/20080705 Firefox/3.0 Kapiko/3.0'
    ]

headers = {'Accept': '*/*',
           'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
           'Accept-Encoding': 'gzip, deflate',
           'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
           'Host': 'www.cninfo.com.cn',
           'Origin': 'http://www.cninfo.com.cn',
           'Referer': 'http://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search&lastPage=index',
           'X-Requested-With': 'XMLHttpRequest'
           }

category = {'category_ndbg_szsh': '年报',
            'category_sjdbg_szsh': '三季报',
            'category_dshgg_szsh': '董事会',
            'category_rcjy_szsh': '日常经营',
            'category_sf_szsh': '首发',
            'category_pg_szsh': '配股',
            'category_kzzq_szsh': '可转债',
            'category_bcgz_szsh': '补充更正',
            'category_tbclts_szsh': '特别处理和退市',
            'category_bndbg_szsh': '半年报',
            'category_yjygjxz_szsh': '业绩预告',
            'category_jshgg_szsh': '监事会',
            'category_gszl_szsh': '公司治理',
            'category_zf_szsh': '增发',
            'category_jj_szsh': '解禁',
            'category_qtrz_szsh': '其他融资',
            'category_cqdq_szsh': '澄清致歉',
            'category_tszlq_szsh': '退市整理期',
            'category_yjdbg_szsh': '一季报',
            'category_qyfpxzcs_szsh': '权益分派',
            'category_gddh_szsh': '股东大会',
            'category_zj_szsh': '中介报告',
            'category_gqjl_szsh': '股权激励',
            'category_gszq_szsh': '公司债',
            'category_gqbd_szsh': '股权变动',
            'category_fxts_szsh': '风险提示'
            }

csv_columns = ['title',
               'stock_short',
               'stock_code',
               'notice_type',
               'notice_time',
               'keyword',
               'enddate',
               'ctime',
               'bbd_xgxx_id',
               'bbd_url',
               'bdd_uptime',
               'bbd_type',
               'bbd_source',
               'bbd_dotime',
               'attachment_url',
               'attachment_list'
               ]

trade = ['农、林、牧、渔业',
         '电力、热力、燃气及水生产和供应业',
         '交通运输、仓储和邮政业',
         '金融业',
         '科学研究和技术服务业',
         '教育',
         '综合',
         '采矿业',
         '建筑业',
         '住宿和餐饮业',
         '房地产业',
         '水利、环境和公共设施管理业',
         '卫生和社会工作',
         '制造业',
         '批发和零售业',
         '信息传输、软件和信息技术服务业',
         '租赁和商务服务业',
         '居民服务、修理和其他服务业',
         '文化、体育和娱乐业'
         ]

plate = {'sz': '深市',
         'szmb': '深主板',
         'szzx': '中小板',
         'szcy': '创业板',
         'sh': '沪市',
         'shmb': '沪主板',
         'shkcp': '科创板'
         }

day_now_seconds = time.strftime('%Y-%m-%d %H:%M:%S')
day_now_days = time.strftime('%Y-%m-%d')

date_start = '2020-06-01'
date_end = day_now_days


def deal_with_url(field, type):
    if 'finalpage' in field:
        words = field.split('/')
        if type == 'date':
            return datetime.datetime.strptime(words[1],"%Y-%m-%d")
        elif type == 'url':
            return words[2]


def single_page(page, p, c, trade, period):
    success_flag = False
    split_flag = False
    headers['User-Agent'] = random.choice(User_Agent)
    query = {'pageNum': page,
             # 页码
             'pageSize': 30,
             # 每页条数
             'tabName': 'fulltext',
             # 结果格式
             'column': '',
             # 列
             'stock': '',
             # 股票代码
             'searchkey': '',
             # 标题关键字
             'secid': '',
             # ID
             'plate': p,
             # 板块
             'category': c,
             # 分类
             'trade': trade,
             # 行业
             'seDate': period
             # 查询时间区段
             }
    query_path = 'http://www.cninfo.com.cn/new/hisAnnouncement/query'
    response = requests.post(query_path, headers=headers, data=query)

    total_size = response.json()['totalAnnouncement']
    if total_size > 3000:
        split_flag = True
        success_flag = False
        print('剪裁时间段：')
        return success_flag,split_flag

    results = response.json()['announcements']

    if results:
        print('-----正在保存,第',page, '页--------')
        success_flag = True
        for result in results:
            notice_time = deal_with_url(result.get('adjunctUrl'), 'date')
            keyword = ''
            if category[c] == '年报':
                keyword = datetime.datetime(notice_time.year + 1, 1, 1) - timedelta(days=1)
            to_csv([[
                result.get('announcementTitle'),
                result.get('secName'),
                result.get('secCode'),
                category[c],
                plate[p],
                trade,
                notice_time,
                '',
                keyword,
                day_now_seconds,
                '',
                'http://www.cninfo.com.cn/new/disclosure/detail?announcementId=' + result.get('announcementId'),
                '',
                'annual_report',
                '巨潮资讯网',
                day_now_days,
                'http://static.cninfo.com.cn/' + result.get('adjunctUrl'),
                ''
            ]])
    else:
        print('-----保存失败，第',page, '页为空----')
    return success_flag, split_flag


def to_csv(row):
    df = pd.DataFrame(row)
    df.to_csv(csv_file_name, sep=',', columns=None, index=False, header=False, mode='a', encoding='utf-8')


def all_pages(plate, category, trade):
    max_pages_num = 100
    i = 1
    period = date_start + '+~+' + date_end
    flag = True
    split = False
    while flag and i <= max_pages_num and not split:
        flag, split = single_page(i, plate, category, trade, period)
        time.sleep(random.randint(0, 2))
        i += 1
    if split:
        start = datetime.datetime.strptime(date_start, '%Y-%m-%d')
        end = datetime.datetime.strptime(date_end, '%Y-%m-%d')
        part = (end - start)/10
        while end - start > part:
            max_pages_num = 100
            i = 1
            period = str(start) + '+~+' + str(start + part)
            print('时间段：', period)
            flag = True
            split = False
            while flag and i <= max_pages_num and not split:
                flag, split = single_page(i, plate, category, trade, period)
                time.sleep(random.randint(0, 2))
                i += 1
            start = start + part
        max_pages_num = 100
        i = 1
        period = str(start) + '+~+' + str(day_now_days)
        print('时间段：', period)
        flag = True
        split = False
        while flag and i <= max_pages_num and not split:
            flag, split = single_page(i, plate, category, trade, period)
            time.sleep(random.randint(0, 2))
            i += 1


# csv_file_name = date_start + '---' + date_end + '.csv'
# if os.path.exists(csv_file_name):
#     print("CSV existed, Continue crawling!")
# else:
#     to_csv([csv_columns])
#
# c_num = 0
# p_num = 0
# t_num = 0
# for c in category:
#     c_num += 1
#     if c_num > 0:
#         for p in plate:
#             p_num += 1
#             if p_num > 0:
#                 for t in trade:
#                     t_num += 1
#                     if t_num > 0:
#                         print('分类：', category[c])
#                         print('板块：', plate[p])
#                         print('行业：', t)
#                         all_pages(p, c, t)

df = pd.read_csv("data/2020-06-01---2021-01-29.csv", index_col=False)
df = df.drop_duplicates(['title','bbd_url','attachment_url'],keep='first',inplace=False)
df.to_csv("2020-06-01---2021-01-29(update).csv", sep=',', columns=None, index=False, mode='a', encoding='utf-8')