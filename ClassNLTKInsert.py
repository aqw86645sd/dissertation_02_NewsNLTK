import pymongo
from bs4 import BeautifulSoup
import datetime
import nltk


class ClassNLTKInsert:
    """
        將 original 的原始新聞資料經過 nltk 及相關資料預處理後塞進 analyze_document

        注意事項：要先確認 資料庫 voo_holding_list 有無資料
    """

    def __init__(self):
        """ DB """
        self.db_client = pymongo.MongoClient("mongodb://localhost:27017/")
        self.coll_voo = self.db_client['python_getStockNews']['voo_holding_list']
        self.coll_analyze = self.db_client['python_getStockNews']['analyze_document']

    def run(self, p_source):

        coll_original = self.db_client['python_getStockNews']["original_" + p_source]

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
                            'isUpdateVIXY': False,
                            'isTotalNewsIdInsert': False
                        }

                        self.coll_analyze.insert_one(insert_data)

                # for 迴圈執行完後表示該 news_id 資料都已塞進 DB，更新 isTotalNewsIdInsert
                update_key = {'source': p_source, 'news_id': news_data['news_id']}
                update_value = {'isTotalNewsIdInsert': True}
                self.coll_analyze.update_many(update_key, {"$set": update_value}, upsert=True)

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
        voo_data = self.coll_voo.find_one()
        voo_holding_list = voo_data['ticker_list']  # 抓全部的ticker

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


if __name__ == '__main__':
    """
        SeekingAlpha
        Zacks
    """
    execute = ClassNLTKInsert()
    execute.run('SeekingAlpha')
