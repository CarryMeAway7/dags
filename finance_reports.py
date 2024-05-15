"""Upload finance reports from Google play and AppsFlayer (IOS) and insert into db"""

import ast
import csv
import datetime
import io
import json
import logging
import os
import requests
import zipfile

import numpy as np
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage

from configs.cred import API_V2_TOKEN_APPSFLYER, PIXELPROTO_EVENTS, PIXELPROTO_MARKETING, CRED_DIR
from db import clickhouse
from db.clickhouse_client import make_request
from utils.dataframe import load_dataframe, dump_frame
from utils.date import get_date_for_dag
from utils.notify import notify


default_args = {
    "owner": "N S",
    "start_date": datetime.datetime(2023, 9, 27),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=60),
    "depends_on_past": False,
}

# Данные по allerts для подключения по API к Slack
SLACK_WEBHOOK_URL = 'SLACK-URL-API'
SLACK_CHANNEL = "#allerts"

# Настройка для комфортного вывода датафрейма в Slack
pd.set_option('display.max_columns', None)


def _send_refund(date, refund):
    """
    Displaying information on returns
    :param date: date
    :param refund: refunds
    """
    data = {
        "text": f'--------------------------{date}--------------------------\n\n'
                f'Возвраты:\n\n {refund}\n'
                f'-------------------------------------------------------------------',
        "username": "Notifications",
        "channel": SLACK_CHANNEL,
    }
    requests.post(SLACK_WEBHOOK_URL, json=data)


def _send_allert(date, count_non_matching: int = 0, diff: pd.DataFrame = pd.DataFrame()):
    """
    Displaying information on script execution
    :param date: date
    :param count_non_matching: number of non-matching lines
    :param diff: mismatched data
    """
    if diff.empty:
        data = {
            "text": f'--------------------------{date}--------------------------\n\n'
                    f'Отсутствуют данные в google play\n'
                    f'-------------------------------------------------------------------',
            "username": "Notifications",
            "channel": SLACK_CHANNEL,
        }
        requests.post(SLACK_WEBHOOK_URL, json=data)
    else:
        data = {
            "text": f'--------------------------{date}--------------------------\n\n'
                    f'Разница строк между таблицей events и google play: {count_non_matching}\n\n'
                    f'Строки, которые не совпали:\n\n {diff}\n'
                    f'-------------------------------------------------------------------',
            "username": "Notifications",
            "channel": SLACK_CHANNEL,
        }
        requests.post(SLACK_WEBHOOK_URL, json=data)


def _send_status(date, status_code):
    """
    Displaying information on processing status
    :param date: date
    :param status_code: upload status
    """
    if status_code == 1:
        data = {
            "text": f'--------------------------{date}--------------------------\n\n'
                    f'Возвраты не обнаружены.\n'
                    f'-------------------------------------------------------------------',
            "username": "Notifications",
            "channel": SLACK_CHANNEL,
        }
        requests.post(SLACK_WEBHOOK_URL, json=data)
    if status_code == 2:
        data = {
            "text": f'--------------------------{date}--------------------------\n\n'
                    f'Данные внесены корректно. Расхождения транзакций не наблюдается.\n'
                    f'-------------------------------------------------------------------',
            "username": "Notifications",
            "channel": SLACK_CHANNEL,
        }
        requests.post(SLACK_WEBHOOK_URL, json=data)


def _send_no_data(date):
    """
    Displaying information on data availability
    :param date: date
    """
    data = {
        "text": f'--------------------------{date}--------------------------\n\n'
                f'Нет данных для внесения в таблицу finance\n'
                f'-------------------------------------------------------------------',
        "username": "Notifications",
        "channel": SLACK_CHANNEL,
    }
    requests.post(SLACK_WEBHOOK_URL, json=data)


@notify()
def _replace_with_content_id(row):
    """
    Take af_content_id from json value
    :param row: row from DF
    :return: af_content_id
    """
    try:
        event_dict = ast.literal_eval(row)
        af_value = event_dict.get("af_content_id")

    except Exception as exception:
        logging.info(
            f"Ошибка при вызове get(af_content_id) из JSON value", f'Ошибка: {exception}'
        )
        raise exception

    return af_value


@notify()
def _replace_with_content(row):
    """
    Take af_content from json value
    :param row: row from DF
    :return: af_content
    """
    try:
        event_dict = ast.literal_eval(row)
        af_value = event_dict.get("af_content")

    except Exception as exception:
        logging.info(
            f"Ошибка при вызове get(af_content) из JSON value", f'Ошибка: {exception}'
        )
        raise exception

    return af_value


@notify()
def _get_android_data(**kwargs):
    """
    Getting finance report from Google play and transform as DataFrame
    :return: Google play's finance report DataFrame
    """
    # function for getting the desired date (yesterday) depending on the date the dag was launched in airflow
    from_date = get_date_for_dag(**kwargs).strftime('%Y-%m-%d')

    start_date = datetime.datetime.strptime(from_date, "%Y-%m-%d")
    start_date = start_date.date()
    to_date = start_date + datetime.timedelta(days=1)

    # convert the function to year-month format
    month_date = str(start_date)
    date_obj = datetime.datetime.strptime(month_date, "%Y-%m-%d")
    month_format = date_obj.strftime("%Y%m")

    # convert the function to year-month format
    bucket_name = 'bucket_name'
    object_name = f'sales/salesreport_{month_format}.zip'
    json_name = os.path.join(CRED_DIR, "cred.json")
    try:
        client = storage.Client.from_service_account_json(json_name)

        # getting bucket and storage
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(object_name)

        # read file with memory
        file_content = blob.download_as_bytes()

        # open file
        with zipfile.ZipFile(io.BytesIO(file_content), 'r') as archive:
            # read csv in archive
            csv_file_name = None
            for file_name in archive.namelist():
                if file_name.lower().endswith('.csv'):
                    csv_file_name = file_name
                    break

            # if csv exists
            if csv_file_name:
                with archive.open(csv_file_name, 'r') as csv_file:
                    reader = csv.reader(io.TextIOWrapper(csv_file, encoding='utf-8'))
                    df1 = pd.DataFrame(reader)

                    if df1.empty:
                        flag_df1 = 'None'
                        logging.info('No android data')
                        _send_allert(from_date)

                        return flag_df1

    except Exception as exception:
        logging.info(
            f"Ошибка при выгрузке финансового отчета {object_name} из {bucket_name}", f'Ошибка: {exception}'
        )
        raise exception

    try:
        df1 = df1.set_axis(df1.iloc[0], axis=1)
        df1 = df1[1:]
        df1 = df1.drop(columns=[
            'Order Charged Date',
            'Device Model',
            'Product Type',
            'SKU ID',
            'City of Buyer',
            'Postal Code of Buyer',
            'Country of Buyer',
            'State of Buyer',
            'Base Plan ID',
            'Offer ID',
            'Group ID',
            'First USD 1M Eligible',
            'Promotion ID',
            'Coupon Value',
            'Discount Rate',
            'Featured Product ID'
        ], axis=1)
        df1 = df1.rename_axis(None, axis=1)

        today_timestamp = datetime.datetime.timestamp(
            datetime.datetime.combine(to_date, datetime.datetime.min.time()))
        yesterday_timestamp = datetime.datetime.timestamp(
            datetime.datetime.combine(start_date, datetime.datetime.min.time()))
        logging.info('time:', today_timestamp, yesterday_timestamp)
        df1["Order Charged Timestamp"] = pd.to_numeric(df1["Order Charged Timestamp"])

        # select in the desired date range
        df1_filtered = df1.loc[df1['Order Charged Timestamp'].between(yesterday_timestamp, (today_timestamp+1800))]
        filter_condition = df1_filtered['Product ID'] == 'Product ID'
        df1_filtered = df1_filtered[filter_condition]

        # filter DataFrame
        df1_filtered['Order Charged Timestamp'] = pd.to_datetime(df1_filtered['Order Charged Timestamp'], unit='s')
        df1_filtered = df1_filtered.rename(columns={'Order Charged Timestamp': 'Event Time'})

        # make new columns
        df1_filtered['Product Title'] = df1_filtered['Product Title'].str.replace("\(Prodict Name\)", "")
        df1_filtered['platform'] = 1

    except Exception as exception:
        logging.info(
            f"Ошибка при фильтрации значений финансового отчета Google play", f'Ошибка: {exception}'
        )
        raise exception

    return dump_frame(df1_filtered)


@notify()
def _get_ios_data(**kwargs):
    """
    Getting finance report from AppsFlyer and transform as DataFrame
    :return: AppsFlyer's finance report DataFrame
    """
    from_date = get_date_for_dag(**kwargs).strftime('%Y-%m-%d')

    # url API AppsFlyer
    url = "IoS-API-URL"

    headers = {
        "accept": "text/csv",
        "authorization": f"Bearer {API_V2_TOKEN_APPSFLYER}"
    }

    params = {
        "from": from_date,
        "to": from_date
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        data = response.content

        logging.info(response)

        df2 = pd.read_csv(io.BytesIO(data))

        if df2.empty:
            df2 = 'None'
            logging.info('No ios data')

            return df2

    except Exception as exception:
        logging.info(
            f"Ошибка при выгрузке финансового отчета из AppsFlyer (ios) за "
            f"{from_date}", f'Ошибка: {exception}'
        )
        raise exception

    try:
        # drop current unused columns
        df2 = df2.drop(columns=[
           'column(s)'
        ], axis=1)
        # make new columns
        df2['Product Title'] = df2['Event Value']
        df2 = df2[df2['Event Value'].str.contains("af_content_id")]
        df2['Event Value'] = df2['Event Value'].apply(_replace_with_content_id)
        df2['Product Title'] = df2['Product Title'].apply(_replace_with_content)

        # rename existing columns
        df2 = df2.rename(columns={
            'Bundle ID': 'Product ID',
            'Event Revenue Currency': 'Currency of Sale',
            'Event Time': 'formatted_datetime'
        })
        df2['platform'] = 0
    except Exception as exception:
        logging.info(
            f"Ошибка при редактировании финансового отчета из AppsFlyer (ios) за "
            f"{from_date}", f'Ошибка: {exception}'
        )
        raise exception

    return dump_frame(df2)


@notify()
def _get_events_data(**kwargs):
    """
    Select data from db
    :return: raw DataFrame from db.events
    """
    dest_table = "table"

    from_date = get_date_for_dag(**kwargs).strftime('%Y-%m-%d')

    start_date = datetime.datetime.strptime(from_date, "%Y-%m-%d")
    to_date = start_date.date() + datetime.timedelta(days=1)

    # converting the received date to timestamp format
    today_timestamp = datetime.datetime.timestamp(
        datetime.datetime.combine(to_date, datetime.datetime.min.time()))
    yesterday_timestamp = datetime.datetime.timestamp(
        datetime.datetime.combine(start_date, datetime.datetime.min.time()))
    yesterday_timestamp = yesterday_timestamp - 900

    logging.info('time:', today_timestamp, yesterday_timestamp)

    try:
        # sql request body
        query = f"""
        SELECT select_param_1, 
            toString(toDateTime(event_timestamp)) AS formatted_datetime,
            select_param_2,
            select_param_3,
            select_param_4
        FROM {dest_table}
        WHERE event_timestamp > {yesterday_timestamp}
            AND event_timestamp < {today_timestamp}
            AND event_id = 'N1'
            AND param_1 = 'N2'
            AND param_2 != 'N3'
            AND param_3 != ''
            """

        # sql request
        df = make_request(query, res_fmt="pandas", config=PIXELPROTO_EVENTS)

        if df.empty:
            flag_df = 'None'
            logging.info('No data events table')

            return flag_df

        # rename columns in sql request
        df = df.rename(columns={
            'json_param_2': 'Order Number'
        })

        # make android's report format of order number for merge (delete json param: "tid")
        df['Order Number'] = df['Order Number'].apply(lambda x: json.loads(x)['tid'])

    except Exception as exception:
        logging.info(
            f"Ошибка при выгрузке финансового отчета из database таблица: {dest_table} за период с "
            f"{yesterday_timestamp} до {today_timestamp}", f'Ошибка: {exception}'
        )
        raise exception

    return dump_frame(df)


@notify()
def _merge_api_android(**kwargs):
    """
    Merge android data + database data
    :return: result of android data + database data
    """
    # if the dataframe return type is try
    try:
        df1 = load_dataframe("af_gp_finance_get_android_data", **kwargs)

        # if there is no data to return, then return None
        if df1.empty:
            df1 = 'None'

            return df1
    # if the return type is not dataframe - exception
    except Exception as exception:
        logging.info(f'Исключение: {exception}')
        df1 = _get_android_data()

        # if there is no data to return, then return None
        if df1 == 'None':
            logging.info('No data for merge android')

            return df1

    # if the dataframe return type is try
    try:
        df3 = load_dataframe("af_gp_finance_get_events_data", **kwargs)

        # если возвращаемых данных нет, то возвращаем None
        if df3.empty:
            df3 = 'None'

            return df3

    # if the return type is not dataframe - exception
    except Exception as exception:
        logging.info(f'Исключение: {exception}')
        df3 = _get_events_data()

        # if there is no data to return, then return None
        if df3 == 'None':
            logging.info('No data for merge events')

            return df3

    # if the received value is not None, but type str, then convert it to dataframe
    if isinstance(df1, str):
        data = json.loads(df1)
        df1 = pd.DataFrame(data)

    if isinstance(df3, str):
        data = json.loads(df3)
        df3 = pd.DataFrame(data)

    if df1.empty:
        df1 = 'None'

        return df1

    date = get_date_for_dag(**kwargs).strftime('%Y-%m-%d')
    spec_date = pd.to_datetime(date)
    # convert to a numeric value
    df1['Charged Amount'] = pd.to_numeric(df1['Charged Amount'], errors='coerce')
    # check for returns and send allerts to slack
    refund = df1[df1['Charged Amount'] < 0]
    if not refund.empty:
        _send_refund(date, refund)
    else:
        _send_status(date, 1)

    # convert time values to one data type
    df1['Event Time'] = pd.to_datetime(df1['Event Time'], unit='ms', errors='coerce')
    df3['formatted_datetime'] = pd.to_datetime(df3['formatted_datetime'])
    df1 = df1[(df1['Event Time'].dt.day == spec_date.day)]
    df3 = df3[(df3['formatted_datetime'].dt.day == spec_date.day)]

    # Merging dataframes using the 'key' field
    merged_df1 = df1.merge(df3, on='Order Number', how='outer')
    merged_df2 = df3.merge(df1, on='Order Number', how='outer')

    df_filtered = merged_df1[merged_df1['Order Number'].str.startswith('GPA')]

    if df_filtered.empty:
        result = pd.DataFrame()
        _send_allert(date, result.shape[0], result)

    # Select rows that have not been concatenated
    unmerged_rows1 = merged_df1[merged_df1['player_id'].isnull()]
    unmerged_rows2 = merged_df2[merged_df2['Item Price'].isnull()]
    result = pd.concat([unmerged_rows1, unmerged_rows2])
    df_filtered = result[result['Order Number'].str.startswith('GPA')]

    if not df_filtered.empty:

        _send_allert(date, result.shape[0], df_filtered)
    else:
        _send_status(date, 2)

    try:
        # merge android data + database data
        merged_df_13 = pd.merge(df1, df3, on='Order Number', how='inner')

        # delete dublicated columns
        merged_df_13 = merged_df_13.drop(columns=[
            'af_id',
            'formatted_datetime'
        ], axis=1)

    except Exception as exception:
        logging.info(
            f"Ошибка при сведении отчетов android data + db data, ios data + db data", f'Ошибка: {exception}'
        )
        raise exception

    return dump_frame(merged_df_13)


@notify()
def _merge_api_ios(**kwargs):
    """
    Merge ios data + database data
    :return: result of ios data + database data
    """
    try:
        df2 = load_dataframe("af_gp_finance_get_ios_data", **kwargs)

    except Exception as exception:
        logging.info(f'Исключение: {exception}')
        df2 = _get_ios_data()

        if df2 == 'None':
            logging.info('No data for merge ios')

            return df2

    try:
        df3 = load_dataframe("af_gp_finance_get_events_data", **kwargs)

    except Exception as exception:
        logging.info(f'Исключение: {exception}')
        df3 = _get_events_data()

        if df3 == 'None':
            logging.info('No data for merge events')

            return df3

    if isinstance(df2, str):
        data = json.loads(df2)
        df2 = pd.DataFrame(data)

    if isinstance(df3, str):
        data = json.loads(df3)
        df3 = pd.DataFrame(data)

    # difference in time from appsflyer and database's table events
    time_diff = 1

    if not df2.empty and not df3.empty:
        try:
            # make timestamp format
            df2['formatted_datetime'] = pd.to_datetime(df2['formatted_datetime']).astype('int64') // 10 ** 9
            df3['formatted_datetime'] = pd.to_datetime(df3['formatted_datetime']).astype('int64') // 10 ** 9

            # merge ios data + database data
            merged_df_23 = pd.merge(df2, df3, on='formatted_datetime', how='inner')

            # delete merged rows
            df2 = df2[~df2['formatted_datetime'].isin(merged_df_23['formatted_datetime'])]
            df3 = df3[~df3['formatted_datetime'].isin(merged_df_23['formatted_datetime'])]

            if not df2.empty and not df3.empty:
                df2['formatted_datetime'] = df2['formatted_datetime'] - time_diff
                merged_df_second = pd.merge(df2, df3, on='formatted_datetime', how='inner')
                merged_df_23_upd = pd.concat([merged_df_23, merged_df_second], ignore_index=True)

                if merged_df_23_upd.empty:
                    merged_df_23_upd = 'None'
                    logging.info('No data for transforming 1st point')

                    return merged_df_23_upd

                return dump_frame(merged_df_23_upd)
            else:

                return dump_frame(merged_df_23)

        except Exception as exception:
            logging.info(
                f"Ошибка при сведении отчетов android ios data + db data",
                f'Ошибка: {exception}'
            )
            raise exception
    else:
        logging.info('No data for transforming 2nd point')
        result_df = 'None'

        return result_df


@notify()
def _filter_merge_ios(**kwargs):
    """
    Filtering ios data
    :return:
    """
    try:
        merged_ios = load_dataframe("af_gp_finance_merge_api_ios", **kwargs)

    except Exception as exception:
        logging.info(f'Исключение: {exception}')
        merged_ios = _merge_api_ios()

        if merged_ios == 'None':
            logging.info('No data for filtering ios')

            return merged_ios

    if isinstance(merged_ios, str):
        data = json.loads(merged_ios)
        merged_ios = pd.DataFrame(data)

    if not merged_ios.empty:
        try:
            # make datetime format
            merged_ios['formatted_datetime'] = pd.to_datetime(merged_ios['formatted_datetime'], unit='s')

            # delete (filter) rows where player id do not equal after merge
            merged_ios = merged_ios.loc[merged_ios['player_id'] == merged_ios['Customer User ID']]
            merged_ios = merged_ios.loc[merged_ios['af_id'] == merged_ios['AppsFlyer ID']]
            merged_ios = merged_ios.loc[merged_ios['Event Value'] == merged_ios['str_param_1']]

            # rename columns
            merged_ios = merged_ios.rename(columns={
                'formatted_datetime': 'Event Time',
                'Event Revenue': 'Item Price'
            })

            # delete dublicated columns
            merged_ios = merged_ios.drop(columns=[
                'af_id',
                'AppsFlyer ID',
                'Customer User ID',
                'Event Value',
                'Event Revenue USD',
            ], axis=1)

        except Exception as exception:
            logging.info(
                f"Ошибка при фильтрации датафреймов android data + db data, ios data + db data",
                f'Ошибка: {exception}'
            )
            raise exception

    return dump_frame(merged_ios)


@notify()
def _merge_result_df(**kwargs):
    """
    Merge and filter Android DataFrame and Ios DataFrame
    :return: result of Merge Android DataFrames and Ios DataFrame
    """
    count_exception = []

    try:
        merged_andro = load_dataframe("af_gp_finance_merge_api_android", **kwargs)

    except Exception as exception:
        logging.info(f'Исключение: {exception}')
        merged_andro = _merge_api_android()

        if merged_andro == 'None':
            count_exception.append('merged_andro')

    try:
        merged_ios = load_dataframe("af_gp_finance_filter_merge_ios", **kwargs)

    except Exception as exception:
        logging.info(f'Исключение: {exception}')
        merged_ios = _filter_merge_ios()

        if merged_ios == 'None':
            count_exception.append('merged_ios')

    if len(count_exception) == 2:
        logging.info('No data for filtering')

        return merged_ios

    if 'merged_ios' in count_exception:
        logging.info('No data for merge ios')

        try:

            if isinstance(merged_andro, str):
                data = json.loads(merged_andro)
                merged_andro = pd.DataFrame(data)

            if not merged_andro.empty:
                result_df = merged_andro
                # make general format of order number (add json param: "tid")
                result_df['Order Number'] = result_df['Order Number'].apply(lambda x: f'{{"tid":"{x}"}}')

                # make general format of item price
                result_df['Item Price'] = result_df['Item Price'].astype(str)
                result_df['Item Price'] = result_df['Item Price'].str.lstrip('0')
                result_df['Item Price'] = result_df['Item Price'].str.strip()
                result_df['Item Price'] = result_df['Item Price'].str.replace(' ', '')
                result_df['Item Price'] = result_df['Item Price'].str.replace(',', '')
                result_df['Item Price'] = pd.to_numeric(result_df['Item Price'], errors='coerce')
                result_df['Item Price'] = result_df['Item Price'].apply(
                    lambda x: '{:.2f}'.format(x) if not pd.isnull(x) else '')

                # make general format of taxes collected
                result_df['Taxes Collected'] = result_df['Taxes Collected'].astype(str)
                result_df['Taxes Collected'] = result_df['Taxes Collected'].str.lstrip('0')
                result_df['Taxes Collected'] = result_df['Taxes Collected'].str.strip()
                result_df['Taxes Collected'] = result_df['Taxes Collected'].str.replace(' ', '')
                result_df['Taxes Collected'] = result_df['Taxes Collected'].str.replace(',', '')
                result_df['Taxes Collected'] = pd.to_numeric(result_df['Taxes Collected'], errors='coerce')
                result_df['Taxes Collected'] = result_df['Taxes Collected'].apply(
                    lambda x: '{:.2f}'.format(x) if not pd.isnull(x) else '')

                # make general format of charged amount
                result_df['Charged Amount'] = result_df['Charged Amount'].astype(str)
                result_df['Charged Amount'] = result_df['Charged Amount'].str.lstrip('0')
                result_df['Charged Amount'] = result_df['Charged Amount'].str.strip()
                result_df['Charged Amount'] = result_df['Charged Amount'].str.replace(' ', '')
                result_df['Charged Amount'] = result_df['Charged Amount'].str.replace(',', '')
                result_df['Charged Amount'] = pd.to_numeric(result_df['Charged Amount'], errors='coerce')
                result_df['Charged Amount'] = result_df['Charged Amount'].apply(
                    lambda x: '{:.2f}'.format(x) if not pd.isnull(x) else '')

                return dump_frame(result_df)

        except Exception as exception:
            logging.info(
                f"Ошибка при попытке сведении всех отчетов в результирующий", f'Ошибка: {exception}'
            )
            raise exception

    elif 'merged_andro' in count_exception:
        logging.info('No data for merge android')

        try:

            if isinstance(merged_ios, str):
                data = json.loads(merged_ios)
                merged_ios = pd.DataFrame(data)

            if not merged_ios.empty:
                result_df = merged_ios
                # make general format of order number (add json param: "tid")
                result_df['Order Number'] = result_df['Order Number'].apply(lambda x: f'{{"tid":"{x}"}}')

                # make general format of item price
                result_df['Item Price'] = result_df['Item Price'].astype(str)
                result_df['Item Price'] = result_df['Item Price'].str.lstrip('0')
                result_df['Item Price'] = result_df['Item Price'].str.strip()
                result_df['Item Price'] = result_df['Item Price'].str.replace(' ', '')
                result_df['Item Price'] = result_df['Item Price'].str.replace(',', '')
                result_df['Item Price'] = result_df['Item Price'].str.replace(r'[^\d.]', '', regex=True)
                result_df['Item Price'] = pd.to_numeric(result_df['Item Price'], errors='coerce')
                result_df['Item Price'] = result_df['Item Price'].apply(
                    lambda x: '{:.2f}'.format(x) if not pd.isnull(x) else '')

                return dump_frame(result_df)

        except Exception as exception:
            logging.info(
                f"Ошибка при попытке сведении всех отчетов в результирующий", f'Ошибка: {exception}'
            )
            raise exception

    else:

        try:
            # concat android and ios dataframes
            result_df = pd.concat([merged_andro, merged_ios], ignore_index=True)

            # make general format of order number (add json param: "tid")
            result_df['Order Number'] = result_df['Order Number'].apply(lambda x: f'{{"tid":"{x}"}}')

            # make general format of item price
            result_df['Item Price'] = result_df['Item Price'].astype(str)
            result_df['Item Price'] = result_df['Item Price'].str.lstrip('0')
            result_df['Item Price'] = result_df['Item Price'].str.strip()
            result_df['Item Price'] = result_df['Item Price'].str.replace(' ', '')
            result_df['Item Price'] = result_df['Item Price'].str.replace(',', '')
            result_df['Item Price'] = result_df['Item Price'].str.replace(r'[^\d.]', '', regex=True)
            result_df['Item Price'] = pd.to_numeric(result_df['Item Price'], errors='coerce')
            result_df['Item Price'] = result_df['Item Price'].apply(
                lambda x: '{:.2f}'.format(x) if not pd.isnull(x) else '')

            # make general format of taxes collected
            result_df['Taxes Collected'] = result_df['Taxes Collected'].astype(str)
            result_df['Taxes Collected'] = result_df['Taxes Collected'].str.lstrip('0')
            result_df['Taxes Collected'] = result_df['Taxes Collected'].str.strip()
            result_df['Taxes Collected'] = result_df['Taxes Collected'].str.replace(' ', '')
            result_df['Taxes Collected'] = result_df['Taxes Collected'].str.replace(',', '')
            result_df['Taxes Collected'] = pd.to_numeric(result_df['Taxes Collected'], errors='coerce')
            result_df['Taxes Collected'] = result_df['Taxes Collected'].apply(
                lambda x: '{:.2f}'.format(x) if not pd.isnull(x) else '')

            # make general format of charged amount
            result_df['Charged Amount'] = result_df['Charged Amount'].astype(str)
            result_df['Charged Amount'] = result_df['Charged Amount'].str.lstrip('0')
            result_df['Charged Amount'] = result_df['Charged Amount'].str.strip()
            result_df['Charged Amount'] = result_df['Charged Amount'].str.replace(' ', '')
            result_df['Charged Amount'] = result_df['Charged Amount'].str.replace(',', '')
            result_df['Charged Amount'] = pd.to_numeric(result_df['Charged Amount'], errors='coerce')
            result_df['Charged Amount'] = result_df['Charged Amount'].apply(
                lambda x: '{:.2f}'.format(x) if not pd.isnull(x) else '')

            return dump_frame(result_df)

        except Exception as exception:
            logging.info(
                f"Ошибка при попытке сведении всех отчетов в результирующий", f'Ошибка: {exception}'
            )
            raise exception


@notify()
def _add_currency(**kwargs):
    """
    Add Exchange currencies to result of Merge Android DataFrames and Ios DataFrame
    :return: result filtered DataFrame
    """
    try:
        merged_df = load_dataframe("af_gp_finance_merge_result_df", **kwargs)

    except Exception as exception:
        logging.info(f'Исключение: {exception}')
        merged_df = _merge_result_df()

        if merged_df == 'None':
            logging.info('No data for merge ios')

            return merged_df

    if isinstance(merged_df, str):
        data = json.loads(merged_df)
        merged_df = pd.DataFrame(data)

    if not merged_df.empty:
        # URL API Central Bank of Russia
        url_cbr = 'https://www.cbr-xml-daily.ru/daily_json.js'

        # URL API currencyapi for currencies not in Central Bank of Russia
        url_others = 'https://api.currencyapi.com/v3/latest?apikey=cur_live_PDsK6RkTymJq4IuStEnewG32fShkUxnsOWJvhcSL'

        try:
            # send get-request JSON to API Central Bank of Russia
            response_cbr = requests.get(url_cbr)
            data_cbr = response_cbr.json()

        except Exception as exception:
            logging.info(
                f"Ошибка при получении response от API ЦБ России", f'Ошибка: {exception}'
            )
            raise exception

        try:
            # send get-request JSON to API currencies not in Central Bank of Russia
            response_others = requests.get(url_others)
            data_others = response_others.json()

        except Exception as exception:
            logging.info(
                f"Ошибка при получении response от currencyapi", f'Ошибка: {exception}'
            )
            raise exception

        try:
            # get Exchange rub to usd
            rub_to_usd_rate = data_cbr['Valute']['USD']['Value']

            # list of currencies
            results = []

            # Loop through each currency in the Valute dictionary
            for currency, values in data_cbr['Valute'].items():
                # Получаем значение и номинал валюты
                value = values['Value']
                nominal = values['Nominal']

                # Calculate the ratio of the value to the nominal value and divide by the rub to usd exchange rate
                result = ((value / nominal) / rub_to_usd_rate)

                # Add the result to the list
                results.append({'Currency of Sale': currency, 'currency_rate': result})

            results.append({'Currency of Sale': 'RUB', 'currency_rate': 1 / rub_to_usd_rate})

            # Create a dataframe from the list of currencies
            df = pd.DataFrame(results)

            # merge merged dataframe + dataframe of currencies
            new_df = pd.merge(merged_df, df, on='Currency of Sale', how='left')

            # make dataframe from response json currencyapi
            df_others = pd.DataFrame.from_dict(data_others['data'], orient='index')

            # replace '' values
            new_df['currency_rate'] = new_df['currency_rate'].replace('', np.nan)

            # merge DF Central Bank of Russia and DF currencyapi
            new_df = new_df.merge(df_others, how='left', left_on='Currency of Sale', right_index=True)

            # make usd currency
            new_df['currency_rate'] = new_df['currency_rate'].fillna(1 / new_df['value'])
            new_df['currency_rate'] = new_df['currency_rate'].astype(float)

            # make usd params
            new_df['Item Price'] = new_df['Item Price'].astype(float)
            new_df['price_usd'] = new_df['Item Price'] * new_df['currency_rate']
            new_df['price_usd'] = new_df['price_usd'].apply(lambda x: round(x, 5))

            if 'Taxes Collected' not in new_df.columns:
                new_df['Taxes Collected'] = pd.Series(dtype=float)
            else:
                new_df['Taxes Collected'] = pd.to_numeric(new_df['Taxes Collected'], errors='coerce').fillna(0)
            new_df['taxes_usd'] = new_df['Taxes Collected'] * new_df['currency_rate']
            new_df['taxes_usd'] = new_df['taxes_usd'].apply(lambda x: round(x, 5))
            new_df['currency_rate'] = new_df['currency_rate'].apply(lambda x: round(x, 5))
            new_df['Currency Date'] = data_cbr['Date']

            # filter filled out values
            if 'Charged Amount' not in new_df.columns:
                new_df['Charged Amount'] = pd.Series(dtype=float)

                new_df[['Taxes Collected', 'taxes_usd']] = new_df.apply(
                    lambda row: pd.Series(['', '']) if row['Charged Amount'] == '' else pd.Series(
                        [row['Taxes Collected'], row['taxes_usd']]), axis=1)
            else:
                new_df[['Taxes Collected', 'taxes_usd']] = new_df.apply(
                    lambda row: pd.Series(['', '']) if row['Charged Amount'] == '' else pd.Series(
                        [row['Taxes Collected'], row['taxes_usd']]), axis=1)

            if 'Financial Status' not in new_df.columns:
                new_df['Financial Status'] = 'None'

            new_df['Taxes Collected'] = new_df['Taxes Collected'].replace('', np.nan)
            new_df['Charged Amount'] = new_df['Charged Amount'].replace('', np.nan)
            new_df['taxes_usd'] = new_df['taxes_usd'].replace('', np.nan)

            # selecting the required cells
            new_df = new_df[[
                'player_id',
                'Event Time',
                'price_usd',
                'taxes_usd',
                'str_param_1',
                'Product Title',
                'Currency of Sale',
                'Item Price',
                'Taxes Collected',
                'Charged Amount',
                'currency_rate',
                'Currency Date',
                'Order Number',
                'Financial Status',
                'Product ID',
                'platform'
            ]]

            # renaming the required cells
            new_df = new_df.rename(columns={
                'Event Time': 'event_time',
                'Order Number': 'order_num',
                'Financial Status': 'fin_status',
                'Product Title': 'product_title',
                'Product ID': 'product_id',
                'Currency of Sale': 'currency_of_sale',
                'Item Price': 'item_price',
                'Taxes Collected': 'taxes',
                'Charged Amount': 'charged_amount',
                'Currency Date': 'currency_date'
            })
        except Exception as exception:
            logging.info(
                f"Ошибка при добавлении columns в конечный датафрейм", f'Ошибка: {exception}'
            )
            raise exception
        new_df['event_time'] = pd.to_datetime(new_df['event_time'], unit='ms')
        # new_df['event_time'] = pd.to_datetime(new_df['event_time']).dt.strftime('%Y-%m-%d %H:%M:%S.%f')
        new_df["currency_date"] = pd.to_datetime(new_df["currency_date"], format="%Y-%m-%dT%H:%M:%S%z").dt.strftime(
            "%Y-%m-%d %H:%M:%S")
        new_df['currency_date'] = pd.to_datetime(new_df['currency_date']).dt.strftime('%Y-%m-%d %H:%M:%S.%f')
        new_df['currency_date'] = new_df['currency_date'].apply(
            lambda x: datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f"))
        new_df['player_id'] = new_df['player_id'].astype('int32')
        new_df['product_title'] = new_df['product_title'].astype(str)
        new_df['currency_of_sale'] = new_df['currency_of_sale'].astype(str)
        new_df['order_num'] = new_df['order_num'].astype(str)
        new_df['fin_status'] = new_df['fin_status'].astype(str)
        new_df['product_id'] = new_df['product_id'].astype(str)
        new_df['price_usd'] = new_df['price_usd'].astype(float)
        new_df['taxes_usd'] = pd.to_numeric(new_df['taxes_usd'], errors='coerce')
        new_df['taxes_usd'] = new_df['taxes_usd'].astype(float)
        new_df['item_price'] = new_df['item_price'].astype(float)
        new_df['taxes'] = pd.to_numeric(new_df['taxes'], errors='coerce')
        new_df['taxes'] = new_df['taxes'].astype(float)
        new_df['charged_amount'] = pd.to_numeric(new_df['charged_amount'], errors='coerce')
        new_df['charged_amount'] = new_df['charged_amount'].astype(float)
        new_df['currency_rate'] = new_df['currency_rate'].astype(float)
        new_df['platform'] = new_df['platform'].astype(int)
        return dump_frame(new_df)

    else:
        merged_df = 'None'

        return merged_df


@notify()
def _insert_data(**kwargs):
    """INSERT finance reports data to DB"""
    try:
        df = load_dataframe("af_gp_finance_add_currency", **kwargs)

    except Exception as exception:
        logging.info(f'Исключение: {exception}')
        df = _add_currency()

        if df == 'None':
            date = get_date_for_dag(**kwargs).strftime('%Y-%m-%d')
            logging.info('No data to insert')
            _send_no_data(date)

            return "there is no data to insert"

    if isinstance(df, str):
        data = json.loads(df)
        df = pd.DataFrame(data)

    if not df.empty:
        client = clickhouse.Client(**PIXELPROTO_MARKETING)
        dest_table = "finance"
        client.execute(
            f"INSERT INTO {dest_table} VALUES",
            df.to_dict(orient="records"),
            types_check=True,
        )
    else:
        logging.info('No data for inserting')


with DAG(
        dag_id="af_gp_finance_reports",
        start_date=datetime.datetime(2023, 9, 27),
        schedule_interval="45 6 * * *",
        max_active_runs=1,
        default_args=default_args,
        catchup=True,
        tags=["marketing"],
) as dag:
    get_android_data = PythonOperator(
        python_callable=_get_android_data, task_id="af_gp_finance_get_android_data", dag=dag
    )

    get_ios_data = PythonOperator(
        python_callable=_get_ios_data, task_id="af_gp_finance_get_ios_data", dag=dag
    )

    get_events_data = PythonOperator(
        python_callable=_get_events_data, task_id="af_gp_finance_get_events_data", dag=dag
    )

    merge_api_android = PythonOperator(
        python_callable=_merge_api_android, task_id="af_gp_finance_merge_api_android", dag=dag
    )

    merge_api_ios = PythonOperator(
        python_callable=_merge_api_ios, task_id="af_gp_finance_merge_api_ios", dag=dag
    )

    filter_merge_ios = PythonOperator(
        python_callable=_filter_merge_ios, task_id="af_gp_finance_filter_merge_ios", dag=dag
    )

    merge_result_df = PythonOperator(
        python_callable=_merge_result_df, task_id="af_gp_finance_merge_result_df", dag=dag
    )

    add_currency = PythonOperator(
        python_callable=_add_currency, task_id="af_gp_finance_add_currency", dag=dag
    )

    insert_data = PythonOperator(
        python_callable=_insert_data, task_id="af_gp_finance_insert_data", dag=dag
    )

get_android_data >> get_ios_data >> get_events_data >> merge_api_android >> merge_api_ios >> filter_merge_ios >> merge_result_df >> add_currency >> insert_data
