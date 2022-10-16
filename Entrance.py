import pymongo
import requests
import re
from ClassNLTKInsert import ClassNLTKInsert


class Entrance:
    def __init__(self):

        """ DB """
        self.db_client = pymongo.MongoClient("mongodb://localhost:27017/")
        self.coll_voo = self.db_client['python_getStockNews']['voo_holding_list']
        self.coll_analyze = self.db_client['python_getStockNews']['analyze_news']

    def run(self):

        """ 更新 VOO 持股到 DB """
        self.reset_voo_holding_list()

        """ 取得 original_SeekingAlpha & original_Zacks 並塞進 analyze_news """
        exe_nltk = ClassNLTKInsert()
        # exe_nltk.run('SeekingAlpha')
        exe_nltk.run('Zacks')

    def reset_voo_holding_list(self):
        """
            更新 VOO 持股到 DB
        """

        """ 刪除舊資料 """
        self.coll_voo.drop()

        """ 抓取最新 VOO 資料並塞進 DB """
        ticker_list = self.get_ticker_list()
        self.coll_voo.insert_one({"ticker_list": ticker_list})

    @staticmethod
    def get_ticker_list():
        """ getting holdings data from Zacks for the given ticker """
        url = "https://www.zacks.com/funds/etf/VOO/holding"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.128 Safari/537.36"
        }

        with requests.Session() as req:
            req.headers.update(headers)
            r = req.get(url)
            tickerList = re.findall(r'etf\\\/(.*?)\\', r.text)

        # 針對有 dot 符號的做處理
        # for idx, ticker in enumerate(tickerList):
        #     if '.' in ticker:
        #         tickerList[idx] = ticker.replace('.', '_')

        return tickerList


if __name__ == '__main__':
    """
        SeekingAlpha
        Zacks
    """

    execute = Entrance()
    execute.run()
