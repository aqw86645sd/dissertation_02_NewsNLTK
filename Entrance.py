import pymongo
import yfinance as yf
import requests
import re
import threading
from ClassNLTKInsert import ClassNLTKInsert


class Entrance:
    def __init__(self):

        """ DB """
        self.db_client = pymongo.MongoClient("mongodb://localhost:27017/")
        self.coll_voo = self.db_client['python_getStockNews']['voo_holding_list']
        self.coll_analyze = self.db_client['python_getStockNews']['analyze_document']

        """ set threading num """
        self.set_thread_num = 100  # 執行序數量

    def run(self):

        """ 更新 VOO 持股到 DB """
        self.reset_voo_holding_list()

        """ 取得 original_SeekingAlpha & original_Zacks 並塞進 analyze_document """
        exe_nltk = ClassNLTKInsert()
        exe_nltk.run('SeekingAlpha')
        exe_nltk.run('Zacks')

        """ 更新各股票資訊 """
        self.update_ticker_data()

        """ 更新 VIXY 資訊 """
        self.update_vixy_data()

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
        for idx, ticker in enumerate(tickerList):
            if '.' in ticker:
                tickerList[idx] = ticker.replace('.', '_')

        return tickerList

    def update_ticker_data(self):
        """
        更新資料（股價）
        :return: None
        """

        # ticker name list (VOO)
        voo_data = self.coll_voo.find_one()
        total_voo_list = voo_data['ticker_list']  # 抓全部的ticker

        """ find len """
        totalLength = len(total_voo_list)

        """ part list to execute thread """
        partList = []
        for n in range(self.set_thread_num):
            partList.append({"tickerList": []})

        for i in range(totalLength):
            """ part tickerList """
            partList[i % self.set_thread_num]["tickerList"].append(total_voo_list[i])

        """ set thread """
        threadList = []
        for p in partList:
            """ start thread """
            thr = threading.Thread(target=self.update_ticker_data_process, args=(p["tickerList"],))
            threadList.append(thr)
            thr.start()

        """ wait total threads finished """
        for thr in threadList:
            thr.join()

    def update_ticker_data_process(self, p_ticker_list):
        """
        依照參數給的 ticker list 更新裡面有得 ticker 資料
        :param p_ticker_list:
        :return:
        """
        for ticker in p_ticker_list:

            try:
                # 有特殊符號的ticker還原處理
                if '_' in ticker:
                    ticker = ticker.replace('_', '.')

                find_update_key = {'ticker': ticker, 'isUpdateTicker': False}
                analyze_data = self.coll_analyze.find(find_update_key)
                analyze_data_list = [row_data for row_data in analyze_data]

                if len(analyze_data_list) > 0:
                    """ 抓股票資料 """
                    ticker_data = yf.Ticker(ticker)
                    year_data = ticker_data.history(period="1y")  # 一年
                    date_list = [date.strftime('%Y-%m-%d') for date in year_data.index]
                    close_price_list = year_data['Close'].tolist()
                    volumn_list = year_data['Volume'].tolist()
                    high_list = year_data['High'].tolist()
                    low_list = year_data['Low'].tolist()

                    total_ticker_date_json = {}

                    for idx in range(len(date_list)):
                        input_json = {}

                        deviation_price = 0
                        deviation_volume = 0

                        if idx > 0:
                            # 當日損益百分比區間誤差(1百分比為一區間)
                            deviation_price = round(
                                (close_price_list[idx] - close_price_list[idx - 1]) / close_price_list[idx - 1] * 100,
                                0)

                        if idx > 3:
                            # 當日成交量與前三日平均成交量變化百分比區間誤差(5百分比為一區間)
                            three_day_average = (volumn_list[idx - 3] + volumn_list[idx - 2] + volumn_list[idx - 1]) / 3
                            deviation_volume = round(
                                (volumn_list[idx] - three_day_average) / three_day_average * 100 / 5,
                                0)

                        # 當日最高與最低股價差異百分比區間誤差(1百分比為一區間) ： 只會是正數
                        deviation_range = round((high_list[idx] - low_list[idx]) / low_list[idx] * 100, 0)

                        input_json['deviation_price'] = deviation_price
                        input_json['deviation_volume'] = deviation_volume
                        input_json['deviation_range'] = deviation_range

                        total_ticker_date_json[date_list[idx]] = input_json

                    """ 將全部資料更新 """
                    for analyze_data in analyze_data_list:
                        if analyze_data['date'] in date_list:

                            update_key = {'source': analyze_data['source'],
                                          'news_id': analyze_data['news_id'],
                                          'date': analyze_data['date'],
                                          'ticker': analyze_data['ticker'],
                                          'news_sentence': analyze_data['news_sentence'],
                                          'isUpdateTicker': False}

                            update_value = total_ticker_date_json[analyze_data['date']]
                            update_value['isUpdateTicker'] = True

                            self.coll_analyze.update_one(update_key, {"$set": update_value}, upsert=True)

                        else:
                            # 假日發的新聞算下一次營業日 todo
                            pass
            except Exception as e:
                print(e)
                pass

    def update_vixy_data(self):

        try:
            find_update_key = {'isUpdateVIXY': False}
            analyze_data = self.coll_analyze.find(find_update_key)
            analyze_data_list = [row_data for row_data in analyze_data]

            if len(analyze_data_list) > 0:
                """ 抓 VIXY 資料 """
                ticker_data = yf.Ticker('VIXY')
                year_data = ticker_data.history(period="1y")  # 一年
                date_list = [date.strftime('%Y-%m-%d') for date in year_data.index]
                close_price_list = year_data['Close'].tolist()
                volumn_list = year_data['Volume'].tolist()

                total_vixy_date_json = {}

                for idx in range(len(date_list)):
                    input_json = {}

                    deviation_vixy_price = 0
                    deviation_vixy_volume = 0

                    if idx > 0:
                        # 當日損益百分比區間誤差(1百分比為一區間)
                        deviation_vixy_price = round(
                            (close_price_list[idx] - close_price_list[idx - 1]) / close_price_list[idx - 1] * 100, 0)

                    if idx > 3:
                        # 當日成交量與前三日平均成交量變化百分比區間誤差(5百分比為一區間)
                        three_day_average = (volumn_list[idx - 3] + volumn_list[idx - 2] + volumn_list[idx - 1]) / 3
                        deviation_vixy_volume = round(
                            (volumn_list[idx] - three_day_average) / three_day_average * 100 / 5, 0)

                    input_json['deviation_vixy_price'] = deviation_vixy_price
                    input_json['deviation_vixy_volume'] = deviation_vixy_volume

                    total_vixy_date_json[date_list[idx]] = input_json

                """ 將全部資料更新 by date """
                for p_date in date_list:

                    update_key = {
                        'date': p_date,
                        'isUpdateVIXY': False
                    }

                    update_value = total_vixy_date_json[p_date]
                    update_value['isUpdateVIXY'] = True

                    self.coll_analyze.update_many(update_key, {"$set": update_value}, upsert=True)

        except Exception as e:
            print(e)
            pass


if __name__ == '__main__':
    """
        SeekingAlpha
        Zacks
    """

    execute = Entrance()
    execute.run()
