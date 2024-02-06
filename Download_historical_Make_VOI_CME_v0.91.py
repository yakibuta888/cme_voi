#!/usr/bin/env python
# coding: utf-8

# 作成：20200806
#
# 更新：20200807, ABLENET環境での実行用にpathを編集
#      20200808, chromedriverをexeファイルに内包する仕様に変更
#      20200808, Make_CME_VOLOI.pyと統合
#      20200809, 日付インデックスを文字列から日付型へ、要素を文字列から数値へ変更
#              , SPDRダウンロードを分離、別プロジェクトへ
#      20200810, cme_listsをcsvファイルから読み込み、自由な銘柄設定ができるように変更
#              , VOLOI_CME_HISTORICAL.xlsxにデータを追記すれば、取り込めるように変更
#              , 既存のデータに新規データを追加する方式へ（シート自体は全て上書きされる）
#      v0.5,20201105, 更新されたFファイルが反映されず、Pファイルのデータが残るバグを修正
#      v0.6,20210209, Fファイルで更新できないPファイルが消去されてしまうバグを修正
#                   , chromedriverを作業ディレクトリに置き、それを使用する仕様に変更
#                   , データに問題があるファイルを読み込んだ時に強制終了するバグを修正
#      v0.7,20210213, 複数銘柄でPファイルの更新がある時にExcel出力でエラー終了するバグを修正
#                   , 活動ログをファイルに書き出す仕様に変更
#      v0.8,20210315, DBから抽出、訂正・消去コマンド、エクセルのみデータをDBデータに追加
#      v0.81,20210316, 新規データ取り込み～DBへ書き込みまで一連のプログラム作成
#      v0.82,20210323, Timeoutエラーに対応（HeadlessMode廃止）、Chromeドライバーの自動更新
#      v0.83,20210324, 更新ファイルが無い時の書き込み作業エラーを修正:add_list
#                    , R,Dコマンドのバグを修正、Win,Linux間のファイルパス互換機能追加
#            20210326, 関数を別ファイルにしてインポートする仕様に変更
#            20210327, 更新データがない時に起こるエラーを修正
#      v0.84,20210521, URLが取得できない→クッキー受け入れ確認ポップに対応、ダウンロードできない→headersの変更（myutil内）
#      v0.9,20210628, cmeサイトのURL変更・サイト仕様変更に対応
#      v0.91,20230811, webdriver_managerのエラーのため、最新版モジュールでリビルド


# In[159]:


import pandas as pd
import sys
import glob
import openpyxl as opx
from bs4 import BeautifulSoup
from lxml import html
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from pathlib import Path
from xlrd import XLRDError
import numpy as np
import sqlite3
from myutil import mpath, log_fprint, download_file, write_excel_sheet, countdowntimer
import time
import os
import re


# In[161]:


# プログラムのエラーをlogファイルへ出力するためのtry
try:
    directory = Path().resolve() #exe化後、実行するディレクトリが代入される

    # ファイルの設定
    export_file_path = os.path.join(directory, 'VOLOI_CME_HISTORICAL.xlsx')
    excel_sheet_name = 'VOI Details Report' #CMEからダウンロードして取り込みたいシート名
    dbname = os.path.join(directory, 'VOI_CME.db') # db\\MARKET_DATA.db


    # In[162]:


    # データを取得したい銘柄の設定
    # download_list_CME.csvという名のcsvファイルにあらかじめ欲しい銘柄を設定してプログラムと同じフォルダに置く

    # CSVファイルの読み込み
    cme_lists = pd.read_csv(os.path.join(directory, 'download_list_CME.csv'), header=None, skiprows =14, encoding='cp932')

    #リスト化
    cme_lists = cme_lists.values.tolist()
    for lst in range(len(cme_lists)):
        cme_lists[lst] = [str(num) for num in cme_lists[lst]] # 全ての要素を文字列化


    # In[160]:


    # Excelシリアル値を日付インデックスに変換する関数
    def exceltime2datetime(et):
        et = int(et)
        if et < 60:
            days = pd.to_timedelta(et - 1, unit='days')
        else:
            days = pd.to_timedelta(et - 2, unit='days')
        return pd.to_datetime('1900/1/1') + days


    # 数値化関数（int優先）
    def to_int_float(x):
        try:
            return int(x)
        except Exception:
            try:
                return float(x)
            except Exception:
                return x


    # In[163]:


    # chromedriverのコンソールログを消すために、プロセス生成時にCREATE_NO_WINDOWフラグを渡す

    from subprocess import Popen, CREATE_NO_WINDOW

    _original_constructor = Popen.__init__

    def _patched_constructor(*args,**kwargs):
        kwargs['creationflags'] = CREATE_NO_WINDOW

        return _original_constructor(*args, **kwargs)


    #---headON mode---------
    # driver = webdriver.Chrome(ドライバーのパス)に変更する

    #---headless mode-----------------------------
    # ブラウザのオプションを格納する変数をもらってきます。
    options = webdriver.ChromeOptions()
    #---headlessで動かすために必要なオプション---
    options.add_argument("--start-maximized")
    options.add_argument("--enable-automation")
    options.add_argument('--headless')
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-infobars")
    options.add_argument('--disable-extensions')
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-browser-side-navigation")
    options.add_argument("--disable-gpu")
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--ignore-ssl-errors')
    options.add_argument("--window-size=1280x1696")
    options.add_argument("--disable-application-cache")
    options.add_argument("--hide-scrollbars")
    options.add_argument("--enable-logging")
    options.add_argument("--log-level=0")
    options.add_argument("--single-process")
    options.add_argument("--homedir=/tmp")
    options.add_argument('--user-agent=hogehoge')
    prefs = {"profile.default_content_setting_values.notifications" : 2}
    options.add_experimental_option("prefs",prefs)

    # ブラウザを起動する
    Popen.__init__ = _patched_constructor # コンソールログを消すおまじない
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
    driver.implicitly_wait(3) # implicitly_wait:指定したドライバーが見つかるまでの待ち時間を設定
    Popen.__init__ = _original_constructor # コンソールログを消すおまじない


    # In[164]:

    log_fprint('Checking the site. Please wait a moment...\n')

    P_update_list = pd.DataFrame(data=None, index=None, columns=None, dtype=None, copy=False)

    for i in range(len(cme_lists)):
        downloadable_list = []
        website_url = 'https://www.cmegroup.com/markets/' + cme_lists[i][3] + '.volume.html#tradeDate='

        # ブラウザでアクセスする
        driver.get(website_url)
        if i == 0:
            try:
                from selenium.common.exceptions import TimeoutException
                WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="pardotCookieButton"]'))).click()
            except TimeoutException as te:
                print('ポップアップ処理のエラー\n')
                print(te)
                pass
        time.sleep(8)

        # HTMLを文字コードをUTF-8に変換してから取得します。
        html = driver.page_source.encode('utf-8')

        # BeautifulSoupで扱えるようにパースします
        soup = BeautifulSoup(html, "html.parser")

        # idやclassで要素を取得し、ダウンロード可能な日付のリストを作成
        trade_date_list = []
        select_date = soup.select("div.dropdown-item > .link")
        for sd in select_date:
            if re.fullmatch(r'([0-9]{8})', sd['data-value']):
                trade_date_list.append(sd['data-value'])
    #     print(trade_date_list)#debug

        # 最新日がFinalかPreliminaryを抽出してリスト化
        # Preliminary day's file have another URL.
        # So this program check and write code the day.
        fin_pre_list = []
        if soup.select(".data-type")[0].text == 'Preliminary Data':
            last_fin_pre = 'P'
        else:
            last_fin_pre = 'F'

        fin_pre_list.append(last_fin_pre)
        fin_pre_list.extend(['F']*4)

    #     print(fin_pre_list)#debug

        df_downloadable = pd.DataFrame(columns=['date', 'Fin_Pre'])

        df_downloadable = df_downloadable.assign(date = trade_date_list)
        df_downloadable = df_downloadable.assign(Fin_Pre = fin_pre_list)
    #     print(df_downloadable)#debug

        # 既存ファイルがあればリストを取得
        import_folder_path = os.path.join(directory, cme_lists[i][0] + mpath('/'))
        ##フォルダの存在確認と新規作成
        if os.path.exists(import_folder_path) == False:
            os.mkdir(import_folder_path)
            log_fprint('Make a ' + cme_lists[i][0] + ' folder.')
        ## ファイルのリストを取得
        excel_files = glob.glob(import_folder_path + "*.xls")

        existing_files = pd.DataFrame(columns=['date', 'Fin_Pre'])
        filedate = []
        fin_pre = []
        for f in excel_files:
            name,ext = os.path.splitext(os.path.basename(f))
            filedate.append(name[:8])
            fin_pre.append(name[-1:])

        existing_files = existing_files.assign(date = filedate)
        existing_files = existing_files.assign(Fin_Pre = fin_pre)

        # CMEからダウンロード可能で、まだ無いファイルリストを抽出
        # days you can download
        df_must_download = df_downloadable.merge(existing_files, indicator=True, how='outer').query('_merge=="left_only"').drop('_merge', 1)
        #print(df_must_download) # debug

        if len(df_must_download) != 0:
            # 過去のPファイルを削除
            df_pre = df_downloadable.merge(existing_files, indicator=True, how='outer').query('Fin_Pre=="P" & _merge=="right_only"').drop('_merge', 1)
            df_delete = df_pre[df_pre['date'].isin(df_downloadable['date'])]
            if len(df_delete) != 0:
                for dele in range(len(df_delete)):
                    del_file_path = os.path.join(directory, cme_lists[i][0] + mpath('/') + df_delete.iloc[dele, 0] + cme_lists[i][1] + '_CME_' + df_delete.iloc[dele, 1] + '.xls')
                    #print(del_file_path) # debug

                    os.remove(del_file_path)
                    log_fprint("Deleted " + df_delete.iloc[dele, 0] + cme_lists[i][1] + "_CME_" + df_delete.iloc[0, 1] + ".xls")


            # CME download
            import_dates = df_must_download['date'].values.tolist()
            cha_f_p = df_must_download['Fin_Pre'].values.tolist()

            for j in range(len(import_dates)):
                url = 'https://www.cmegroup.com/CmeWS/exp/voiProductDetailsViewExport.ctl?media=xls&tradeDate=' + import_dates[j] + '&reportType=' + cha_f_p[j] + '&productId=' + cme_lists[i][2]
                export_file = os.path.join(directory, cme_lists[i][0] + mpath('/') + import_dates[j] + cme_lists[i][1] + '_CME_' + cha_f_p[j] + '.xls')
                download_file(url, export_file)
                #print(url)  # debug
                log_fprint("Got " + import_dates[j] + cme_lists[i][1] + "_CME_" + cha_f_p[j] + ".xls")
        else:
            log_fprint(cme_lists[i][1] + ' does not have the latest files.')


    # ブラウザの終了(headON modeの時)
    if driver is not None:
        driver.quit()

    log_fprint("\n""Succeeded in CMEfiles downloading.""\n")


    # In[17]:
    # Vol,OIをExcelに書き出すスクリプト

    # DB access
    conn = sqlite3.connect(dbname)

    for ass in range(len(cme_lists)):
        # 存在するファイルのリストを取得
        import_folder_path = os.path.join(directory, cme_lists[ass][0] + mpath('/'))
        excel_files = glob.glob(import_folder_path + "*.xls")

        filedate = []
        fin_pre = []
        add_list = []
        for f in excel_files:
            name,ext = os.path.splitext(os.path.basename(f))
            filedate.append(name[:8])
            fin_pre.append(name[-1:])

        daily_files = pd.Series(fin_pre, index=filedate, name='Fin_Pre')

        df_voloi = pd.DataFrame(columns=['Total_Volume', 'Open_Interest', 'Fin_Pre'])# dfのリセット

        # DBから既存のVolOIリストがあれば抽出
        table_name = 'VOI_CME_' + cme_lists[ass][1]
        sql = 'select * from ' + table_name

        try:
            db_voi = pd.read_sql(sql, conn).set_index('index')
            db_voi.index = pd.to_datetime(db_voi.index, format='%Y-%m-%d') # インデックスをDateTime型へ
        except Exception: # DBがない
            db_voi = pd.DataFrame(columns=['Total_Volume', 'Open_Interest', 'Fin_Pre'])# dfのリセット

        finally:
            raw_db = db_voi.copy()

        # print('DB data\n', db_voi)#debug
        # 既存のVolOIリストがあればExcelから抽出
        if os.path.exists(export_file_path) == True:
            try:
                exist_voloi = pd.read_excel(export_file_path, sheet_name=cme_lists[ass][1], index_col=0)

            except XLRDError as e: # sheetが無い
                log_fprint(e, lend=', so ')

                if len(db_voi.index) != 0: # DBのデータが存在するなら
                    df_voloi = db_voi.copy()

                else: # 新規作成
                    add_list = daily_files.index

            else:
                # 日付が数値型かどうか判定
                is_serial_value = exist_voloi.index.astype('str').str.isdigit()

                # 数値型の箇所をDateTime型に変換
                for e2d in exist_voloi[is_serial_value].index:
                    exist_voloi.rename(index= {e2d: exceltime2datetime(e2d)}, inplace=True)
                #print(exist_voloi)#debug

                # 文字列の箇所をDateTime型に変換
                for s2d in exist_voloi[~is_serial_value].index:
                    exist_voloi.rename(index= {s2d: pd.to_datetime(s2d, errors='coerce')}, inplace=True)
                #print(exist_voloi)#debug
                #print(daily_files)#debug

                # 要素の中に数値以外があれば数値化を試して、数値にならなければ警告する（Fin_Preカラムを除く）
                for chelem in exist_voloi.columns[0:2]:
                    chenum = 0
                    while chenum < 2:
                        # 要素のチェック
                        exist_voloi[chelem] = exist_voloi[chelem].apply(to_int_float) # 数値化
                        check_s = exist_voloi[chelem].apply(lambda s:pd.to_numeric(s, errors='coerce')).notnull().all()
                        chenum += 1
                        if check_s == False and chenum < 2: # 数値化
                            exist_voloi[chelem] = exist_voloi[chelem].astype(str).str.replace(',', '')
                            exist_voloi[chelem] = pd.to_numeric(exist_voloi[chelem], errors='coerce')

                        elif check_s == False: # 数値化できない時の警告
                            log_fprint('*********************************   Warning   **********************************')
                            log_fprint('\nExcel file of "' + cme_lists[ass][1] + ': ' + chelem + \
                                '" has incorrect data that cannot be analyzed, such as character strings and symbols.\n'\
                                'Please enter the numerical data.\n')
                            log_fprint('********************************************************************************')

                        else: # 問題ないデータ
                            break
                    #print(chelem + str(chenum)) # debug チェックカラム＋回数

                # Rフラグ（訂正コマンド）のデータをDBに上書き
                for r_index in exist_voloi[exist_voloi['Fin_Pre'].isin(['R'])].index:
                    try:
                        #print('RコマンドのあるExcelデータ\n', exist_voloi.loc[r_index, :])#debug
                        #print('Rコマンドの日付に対応するDBデータ\n', db_voi.loc[r_index, :])#debug
                        db_voi.loc[r_index, :'Open_Interest'] = exist_voloi.loc[r_index, :'Open_Interest']
                        db_voi.loc[r_index, 'Fin_Pre'] = 'E' # Excelデータのフラグ
                        exist_voloi = exist_voloi[exist_voloi.loc[r_index, 'Fin_Pre'] != 'R'] # 処理後のRデータを消す

                    except KeyError: # DBにないExcelデータにRフラグがあるとエラーになるのを回避
                        exist_voloi.loc[r_index, 'Fin_Pre'] = 'E' # Excelデータのフラグ

                # print("Replaced DB data\n", db_voi)#debug
                # エクセルから抽出したVolOIリストのうち、DBに無いデータ（手入力）があれば追加する
                excel_only = exist_voloi[~exist_voloi.isin(db_voi.to_dict(orient='list')).all(1)]
                # print('Excelのみのデータ\n', excel_only)#debug
                # Dフラグ（消去コマンド）の行を削除
                if len(exist_voloi[exist_voloi['Fin_Pre'] == 'D'].index) != 0:
                    db_voi.drop(exist_voloi[exist_voloi['Fin_Pre'] == 'D'].index, inplace=True)
                    excel_only = excel_only[excel_only['Fin_Pre'] != 'D']
                # print("Deleted DB's data\n", db_voi)#debug
                #excel_only = pd.DataFrame(columns=['Total Volume', 'Open Interest', 'Fin_Pre'])# テスト用初期化
                # print('Deleted Excelのみのデータ\n', excel_only)#debug
                db_voi = db_voi.append(excel_only)
                db_voi.sort_index(inplace=True, ascending=False)
                db_voi = db_voi.fillna({'Fin_Pre': 'E'}) # Excelデータのフラグ
                # print('Excelデータを追加したDBデータ\n', db_voi)#debug

                # 新たに取り込むべき日のリスト：daily_filesにあってexist_voloi db_voi にないもの
                add_list2 = list(set(daily_files.index) - set(db_voi.index.strftime('%Y%m%d')))
                # print('データ化していないもの\n', add_list2)#debug
                # データ化していないもののみの抽出なので、F優先の組み合わせを別途考えなくてはならない

                # 比較のためにインデックスを文字列化する
                reinx_db_voi = db_voi.reset_index()
                reinx_db_voi['index'] = reinx_db_voi['index'].dt.strftime('%Y%m%d')
                # daily_filesとdb_voi両方にあるもの
                df_both = pd.merge(reinx_db_voi, daily_files.reset_index(), left_on=['index', 'Fin_Pre'], right_on=['index', 'Fin_Pre'], how='inner').head(10)
                # print(df_both)#debug

                # Pファイルが F（E）データより優先されてしまう問題がある
                # Pファイルを優先させないために、対象外にする日付のリスト
                p_daily_files = daily_files[daily_files == 'P']
                # CMEからダウンロードしたファイル内容で上書きする日付のリスト（一度未データ化の重複を削除）
                pre_add_list = list(set(daily_files.index) - set(df_both['index']) - set(p_daily_files.index) - set(add_list2))
                # （結合）未データ化と上書きFファイルのリスト
                add_list = list(set(pre_add_list + add_list2))

                # Fデータで更新するP,Eデータを削除
                df_voloi = db_voi.copy()
                df_voloi.drop(index=pd.to_datetime(pre_add_list), inplace=True)
                # print('Fデータで更新しないデータ（残すデータ）これにCMEのExcelデータを足す\n', df_voloi)#debug

        elif len(db_voi.index) != 0: # DBのデータが存在するなら
            df_voloi = db_voi.copy()

        else: # 新規作成
            add_list = daily_files.index

        df_voloi.dropna(how='all', inplace=True)

        # 更新が必要な場合はVolOIリストを作成して書き込む
        if len(add_list) != 0:
            for g in add_list:
                if g is np.nan:
                    continue # nanエラーを回避
                try:
                    # ファイルデータ読み込みとVolOIの抽出
                    df_order = pd.read_excel(import_folder_path + g + cme_lists[ass][1] + '_CME_' + daily_files[g] + '.xls', sheet_name = excel_sheet_name, skiprows = 5)

                    df_order = df_order[df_order['Month'].isin(['TOTALS'])].head(1)
                    df_order = df_order.set_index('Month')

                    df_order = df_order.loc['TOTALS', ['Total Volume', 'At Close']]

                    df_order2 = pd.DataFrame([df_order], columns=['Total Volume', 'At Close'], index=['TOTALS'])

                    # 日付をインデックスにする
                    cfd = g[:4] + '-' + g[4:6] + '-' + g[6:]
                    cfd2 = pd.to_datetime(cfd, format='%Y-%m-%d')
                    df_order2 = df_order2.rename(columns={'Total Volume': 'Total_Volume', 'At Close': 'Open_Interest'}, index={'TOTALS': cfd2})
                    df_order2['Fin_Pre'] = daily_files[g]

                    # データ追加
                    df_voloi = pd.concat([df_order2, df_voloi])

                except KeyError:
                    pass

            # 最新日から降順にソート
            df_voloi.sort_index(inplace=True, ascending=False)

            # 要素を文字列から数値へ変換
            col_list = df_voloi.columns.values.tolist()
            for colist in col_list[0:2]: # Fin_Preは除く
                df_voloi[colist] = df_voloi[colist].astype(str).str.replace(',', '')
                df_voloi[colist] = pd.to_numeric(df_voloi[colist], errors='coerce')

        # print('書き込むデータ\n', df_voloi)#debug
        #print(type(df_voloi))#debug
        #print(df_voloi.dtypes)#debug

        # DBに書きこむべき更新データが無ければ、Excel,DB共に書き込み作業をしない
        if not raw_db.equals(df_voloi):
            # ファイルに書き込み
            write_excel_sheet(df_voloi, export_file_path, cme_lists[ass][1])

            # DBに書き込み（上書き）
            df_voloi.to_sql(table_name, conn, if_exists = 'replace') #同じテーブル名が存在する場合の動作

        else:
            log_fprint('There is no data to update in ' + cme_lists[ass][1] + '.')

    # DB close
    conn.close()

    log_fprint('\n''Finished making all CME Vol_OI sheets and DataBase.')
    print('\n''You can close this window.')
    countdowntimer(1, 30)


except Exception as e:
    import traceback
    import datetime

    # ログのファイル名にする日付を定義
    today = datetime.date.today().strftime('%Y%m%d')

    # fileを指定しない場合はsys.stderr(標準エラー)に出力
    with open(os.path.join(directory, mpath('log/') + today + '.log'), 'a') as f:
        print('エラー情報：', file=f)
        traceback.print_exc(file=f)


# In[ ]:
