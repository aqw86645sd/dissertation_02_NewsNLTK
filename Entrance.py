import pymongo
from bs4 import BeautifulSoup
import datetime
import nltk
import yfinance as yf
import requests
import re
import threading


class Entrance:
    def __init__(self):

        """ DB """
        self.db_client = pymongo.MongoClient("mongodb://localhost:27017/")
        self.coll_analyze = self.db_client['python_getStockNews']['analyze_document']

        """ set threading num """
        self.set_thread_num = 100  # 執行序數量

    def run_original_to_analyze(self):

        # news source list
        news_source_list = ["SeekingAlpha", "Zacks"]

        for p_source in news_source_list:
            # 兩種新聞來源都要做
            collection_name = "original_" + p_source
            coll_original = self.db_client['python_getStockNews'][collection_name]

            # 取得全部資料
            coll_original_data = coll_original.find()  # total news
            news_data_list = [row_data for row_data in coll_original_data]

            # 反轉list，方便判斷是否已寫進DB
            news_data_list.reverse()

            for news_data in news_data_list:

                # 判斷“分析資料”是否已經存在DB，存在的話直接exit
                query_key = {'source': p_source, 'news_id': news_data['news_id']}

                if self.coll_analyze.find_one(query_key):
                    # data existed
                    break
                else:
                    """ 對資料正規劃 """

                    """ 原始資料 """
                    # 新聞內容
                    news_content_text = ''  # 完整新聞內容
                    news_date = ''  # 時間 yyyy-mm-dd
                    if p_source == 'Zacks':
                        news_content_text = news_data['content']
                        news_date = datetime.datetime.strptime(news_data['date'], "%d/%m/%Y").strftime("%Y-%m-%d")
                    elif p_source == 'SeekingAlpha':
                        content = news_data['content']
                        soup = BeautifulSoup(content, "html.parser")
                        news_content_text = soup.text
                        news_date = news_data['date'][0:10]

                    """ 預處理 """
                    # 特殊字符移除
                    news_content_text = self.replace_special_word(news_content_text)

                    # 調整字串
                    news_content_text = self.alter_text_for_sentence(news_content_text)

                    # Sentence Segmentation (斷句)
                    sentence_list = nltk.sent_tokenize(news_content_text)

                    # Word Segmentation (斷詞)
                    total_token_list = [nltk.tokenize.word_tokenize(sentence) for sentence in sentence_list]

                    # Lemmatization (字型還原-簡易版）
                    total_lemmatization_list = []
                    for token_list in total_token_list:
                        lemmatization_list = [self.lemmatize(token) for token in token_list]
                        total_lemmatization_list.append(lemmatization_list)

                    # POS (詞性標記)
                    total_pos_list = [nltk.pos_tag(lemmatization_list) for lemmatization_list in
                                      total_lemmatization_list]

                    # 使用詞性抓出ticker
                    identify_ticker_list = self.identify_ticker_with_pos(total_pos_list)

                    # 建立分析資料 to DB
                    for idx, ticker_list in enumerate(identify_ticker_list):
                        for ticker in ticker_list:
                            insert_data = {
                                'source': p_source,
                                'news_id': news_data['news_id'],
                                'date': news_date,
                                'ticker': ticker,
                                'news_sentence': total_lemmatization_list[idx],
                                'isUpdateTicker': False,
                                'isUpdateVIXY': False
                            }

                            self.coll_analyze.insert_one(insert_data)

    def run_update_analyze(self):
        """
        更新資料（股價）
        :return: None
        """
        total_voo_list = self.get_ticker_list()

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
            thr = threading.Thread(target=self.update_analyze_process, args=(p["tickerList"],))
            threadList.append(thr)
            thr.start()

        """ wait total threads finished """
        for thr in threadList:
            thr.join()

    @staticmethod
    def replace_special_word(text):
        """ 特殊字元會造成分詞誤判時，先修改成特定字元
        :param text:
        :return:
        """
        # 特殊字元清單
        text = text.replace("S&P 500", "Standard_and_Poor's_500")
        text = text.replace("PEG ratio", "PEG_ratio")
        text = text.replace("P/E ratio", "P/E_ratio")
        text = text.replace("&", "_and_")
        text = text.replace("No. ", "No.")

        # 特殊 ticker
        text = text.replace("BRK.B", "BRK_B")

        return text

    @staticmethod
    def alter_text_for_sentence(text):
        """
        調整字串，方便斷句
        :param text:
        :return:
        """
        dot_split_list = text.split(".")
        alter_text = ""  # 修改後字串

        for n in dot_split_list:
            if n:
                if n[0].isupper():
                    # 開頭為大寫，則增加空白
                    alter_text += " "
                alter_text += n + "."

        return alter_text

    def identify_ticker_with_pos(self, total_pos_list):
        """
        利用詞性抓出股票代號
        :param total_pos_list: 已經做好的詞性list
        :return:
        """

        """ STEP1 抓出 VOO 持股 """
        # ticker name list (VOO)
        voo_holding_list = self.get_ticker_list()  # 抓全部的ticker

        ticker_in_total_list = []  # 全部 Sentence 裡的 ticker

        for pos_list in total_pos_list:

            ticker_in_sentence_list = []  # 單一 Sentence 有的 ticker

            for token, tag in pos_list:
                if tag.startswith('NNP'):
                    # 為專有名詞縮寫
                    if token in voo_holding_list:
                        """ STEP2 新增到單句 ticker list """
                        if token not in ticker_in_sentence_list:
                            ticker_in_sentence_list.append(token)

            """ STEP3 新增到全部文章 ticker list """
            if ticker_in_sentence_list:
                # 有 ticker 資料
                ticker_in_total_list.append(ticker_in_sentence_list)
            else:
                # 當該 Sentence 沒有 ticker，則須沿用上次ticker
                if ticker_in_total_list:
                    ticker_in_total_list.append(ticker_in_total_list[-1])
                else:
                    ticker_in_total_list.append([])

        """ STEP4 若前面部分沒資料，則用後面最近資料補上 """
        current_idx = 0
        current_ticker_data = []
        for idx, ticker_data in enumerate(ticker_in_total_list):
            if ticker_data:
                current_idx = idx
                current_ticker_data = ticker_data
                break

        # 更新無ticker的資料
        for i in range(current_idx):
            ticker_in_total_list[i] = current_ticker_data

        return ticker_in_total_list

    @staticmethod
    def lemmatize(word):
        """
        字型還原-只還原動詞
        :param word: 單字
        :return:
        """
        lemmatizer = nltk.stem.wordnet.WordNetLemmatizer()
        lemma = lemmatizer.lemmatize(word, 'v')
        return lemma

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

    def update_analyze_process(self, p_ticker_list):
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
                                (close_price_list[idx] - close_price_list[idx - 1]) / close_price_list[idx - 1] * 100, 0)

                        if idx > 3:
                            # 當日成交量與前三日平均成交量變化百分比區間誤差(5百分比為一區間)
                            three_day_average = (volumn_list[idx - 3] + volumn_list[idx - 2] + volumn_list[idx - 1]) / 3
                            deviation_volume = round((volumn_list[idx] - three_day_average) / three_day_average * 100 / 5,
                                                     0)

                        # 當日最高與最低股價差異百分比區間誤差(1百分比為一區間) ： 只會是正數
                        deviation_range = round((high_list[idx] - low_list[idx]) / low_list[idx] * 100, 0)

                        input_json['deviation_price'] = deviation_price
                        input_json['deviation_volume'] = deviation_volume
                        input_json['deviation_range'] = deviation_range

                        total_ticker_date_json[date_list[idx]] = input_json

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


if __name__ == '__main__':
    """
        SeekingAlpha
        Zacks
    """

    execute = Entrance()

    # 取得 original_SeekingAlpha & original_Zacks 並塞進 analyze_document
    execute.run_original_to_analyze()

    # 更新股票相關資料
    execute.run_update_analyze()
