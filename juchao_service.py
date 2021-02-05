# -*- coding:utf-8 -*-
import datetime
import pandas as pd
import requests
import random
import time
import math
from sqlalchemy import create_engine
from datetime import timedelta
from json.decoder import JSONDecodeError


def config():
    User_Agent = ['Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)',
                  'Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 1.0.3705; .NET CLR 1.1.4322)',
                  'Mozilla/4.0 (compatible; MSIE 7.0b; Windows NT 5.2; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.2; .NET CLR 3.0.04506.30)',
                  'Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN) AppleWebKit/523.15 (KHTML, like Gecko, Safari/419.3) Arora/0.3 (Change: 287 c9dfb30)',
                  'Mozilla/5.0 (X11; U; Linux; en-US) AppleWebKit/527+ (KHTML, like Gecko, Safari/419.3) Arora/0.6',
                  'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.2pre) Gecko/20070215 K-Ninja/2.1.1',
                  'Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9) Gecko/20080705 Firefox/3.0 Kapiko/3.0'
                  ]
    Headers = {'Accept': '*/*',
               'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
               'Accept-Encoding': 'gzip, deflate',
               'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
               'Host': 'www.cninfo.com.cn',
               'Origin': 'http://www.cninfo.com.cn',
               'Referer': 'http://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search&lastPage=index',
               'X-Requested-With': 'XMLHttpRequest'
               }
    Category = {'category_ndbg_szsh': '年报',
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
    Column_name = ['title',
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
    return User_Agent, Headers, Category, Column_name


def get_time_period():
    today = str(datetime.date.today()) + " 00:00:00"
    yesterday = str(datetime.date.today() - datetime.timedelta(days = 1)) + " 00:00:00"
    period = yesterday + ' ~ ' + today
    return period


def single_page(page, category, period, user_agent, headers, categories):
    headers['User-Agent'] = random.choice(user_agent)
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
             'plate': 'sz;szmb;szzx;szcy;sh;shmb;shkcp',
             # 板块
             'category': category,
             # 分类
             'trade': '农、林、牧、渔业;电力、热力、燃气及水生产和供应业;交通运输、仓储和邮政业;金融业;科学研究和技术服务业;教育;综合;采矿业;'
                      '建筑业;住宿和餐饮业;水利、环境和公共设施管理业;卫生和社会工作;房地产业;制造业;批发和零售业;'
                      '信息传输、软件和信息技术服务业;租赁和商务服务业;居民服务、修理和其他服务业;文化、体育和娱乐业',
             # 行业
             'seDate': period
             # 查询时间区段
             }
    query_path = 'http://www.cninfo.com.cn/new/hisAnnouncement/query'
    response = requests.post(query_path, headers=headers, data=query)
    total_size = response.json()['totalAnnouncement']
    results = response.json()['announcements']
    page_data = []
    if results:
        for result in results:
            title = result.get('announcementTitle')
            stock_short = result.get('secName')
            stock_code = result.get('secCode')
            notice_type = categories[category]
            notice_time = datetime.datetime.strptime(result.get('adjunctUrl').split('/')[1],'%Y-%m-%d')
            keyword = ''
            enddate = ''
            if categories[category] == '年报':
                enddate = (datetime.datetime(notice_time.year + 1, 1, 1) - timedelta(days=1)).strftime('%Y-%m-%d')
            ctime = time.strftime('%Y-%m-%dT%H:%M:%S.000+08:00')
            bbd_xgxx_id = ''
            bbd_url = 'http://www.cninfo.com.cn/new/disclosure/detail?announcementId=' + result.get('announcementId'),
            bdd_uptime = ''
            bbd_type = 'annual_report'
            bbd_source = '巨潮资讯网'
            bbd_dotime = time.strftime('%Y-%m-%d')
            attachment_url = 'http://static.cninfo.com.cn/' + result.get('adjunctUrl')
            attachment_list = ''
            row = [title,
                   stock_short,
                   stock_code,
                   notice_type,
                   notice_time.strftime('%Y-%m-%d'),
                   keyword,
                   enddate,
                   ctime,
                   bbd_xgxx_id,
                   bbd_url,
                   bdd_uptime,
                   bbd_type,
                   bbd_source,
                   bbd_dotime,
                   attachment_url,
                   attachment_list
                   ]
            page_data.append(row)
    return page_data, total_size,


def insert_into_mysql(data_frame):
    try:
        engine = create_engine("mysql+pymysql://qy_listed:Xg=pfAP*bPR3#EK@192.168.28.101:3306/qy_listed?charset=utf8")
        data_frame.to_sql(name = 'manage_cninfo_daily',con = engine,if_exists = 'append',index = False,index_label = False)
    except Exception as error:
        print("       插入数据库失败.原因是:{}".format(error))


if __name__ == "__main__":
    user_agent, headers, categories, column_name = config()
    period = get_time_period()
    print("\n--------开始爬取{}的数据--------".format(period[:10]))
    all_data = []
    for c in categories:
        try:
            data, total_size = single_page(1, c, period, user_agent, headers, categories)
            all_data += data
            print("     正在爬取:{}, 条目数为:{}".format(categories[c],total_size))
            for i in range(2, math.floor(total_size / 30) + 2):
                data, single_size = single_page(i, c, period, user_agent, headers, categories)
                all_data += data
        except JSONDecodeError:
            print("       {}, 爬取失败.该分类下条目数过多,仅能爬取前3000条".format(categories[c]))
        except Exception as err:
            print("       {}, 爬取失败.原因是:{}".format(categories[c], err))
    result = pd.DataFrame(all_data,columns=column_name)
    result.drop_duplicates(["bbd_url"], keep='first', inplace=True)
    insert_into_mysql(result)
    print("--------结束爬取{}的数据--------".format(period[:10]))