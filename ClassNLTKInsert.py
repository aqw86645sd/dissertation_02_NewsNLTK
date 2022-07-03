import pymongo
from bs4 import BeautifulSoup
import datetime
import nltk


class ClassNLTKInsert:
    """
        將 original 的原始新聞資料經過 nltk 及相關資料預處理後塞進 analyze_news

        注意事項：要先確認 資料庫 voo_holding_list 有無資料
    """

    def __init__(self):
        """ DB """
        self.db_client = pymongo.MongoClient("mongodb://localhost:27017/")
        self.coll_voo = self.db_client['python_getStockNews']['voo_holding_list']
        self.coll_analyze = self.db_client['python_getStockNews']['analyze_news']

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
                print('資料已經到達上次更新位置')
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

                # POS (詞性標記)
                total_pos_list = [nltk.pos_tag(token_list) for token_list in total_token_list]

                # Lemmatization (字型還原）& 大寫字母轉小寫字母
                total_lemmatization_list = []  # 完整的句子
                for pos_list in total_pos_list:
                    lemmatization_list = [self.lemmatize_by_pos(token, pos) for token, pos in pos_list]
                    total_lemmatization_list.append(lemmatization_list)

                # 使用詞性抓出ticker
                identify_ticker_list = self.identify_ticker_with_pos(total_pos_list)

                insert_data_list = []  # 新增資料list
                length_sentence = len(identify_ticker_list)  # 總句子數

                """
                    sequence 邏輯
                    日期(8碼) + news_id(向左捕0到9碼) + sentence_seq (單一新聞內容中句子的seq:向左捕0到6碼)  
                """
                sentence_seq = 0

                # 建立分析資料 to DB
                for idx1, ticker_list in enumerate(identify_ticker_list):
                    for idx2, ticker in enumerate(ticker_list):

                        # 使用已還原動詞的單字組成完整句子
                        news_sentence_str = ''
                        for word in total_lemmatization_list[idx1]:
                            news_sentence_str += word + ' '

                        insert_data = {
                            'sequence': news_date.replace('-', '') + news_data['news_id'].zfill(9) + str(sentence_seq).zfill(6),
                            'source': p_source,
                            'news_id': news_data['news_id'],
                            'date': news_date,
                            'ticker': ticker,
                            'news_sentence_str': news_sentence_str,
                            'news_sentence_list': total_lemmatization_list[idx1],
                            'news_sentence_with_pos_list': total_pos_list[idx1]
                        }

                        insert_data_list.append(insert_data)

                        sentence_seq += 1  # seq加一

                        if idx1 == length_sentence - 1 and idx2 == len(ticker_list) - 1:
                            # 該 news_id 資料都彙整成一個 list 一起新增
                            self.coll_analyze.insert_many(insert_data_list)
                            print(insert_data_list)

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
    def lemmatize_by_pos(token, pos):
        """
        字型還原 ＆ 大寫轉小寫
        :param token: 單字
        :param pos: 詞性
        :return:
        """
        word = token.lower()

        lemmatizer = nltk.stem.wordnet.WordNetLemmatizer()
        if pos.startswith('J'):
            return lemmatizer.lemmatize(word, 'a')
        elif pos.startswith('V'):
            return lemmatizer.lemmatize(word, 'v')
        elif pos.startswith('N'):
            return lemmatizer.lemmatize(word, 'n')
        elif pos.startswith('R'):
            return lemmatizer.lemmatize(word, 'r')
        else:
            return word


if __name__ == '__main__':
    """
        SeekingAlpha
        Zacks
    """
    execute = ClassNLTKInsert()
    execute.run('SeekingAlpha')
