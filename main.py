import json
import requests
import datetime
import pymssql
import time
import sys

att_token = ''
token_expires = datetime.datetime.now()


def read_connection_params():
    with open('conn.json', 'r', encoding='utf8') as f:
        return json.loads(f.read())


def write_status(guid, status):
    conn2 = pymssql.connect(**read_connection_params())
    with conn2.cursor(as_dict=True, ) as update:
        update.execute('update wares_gtins set check_result = %s where guid = %s', (status, guid))


def update_api_token(req):
    global token_expires, att_token
    try:
        if token_expires <= datetime.datetime.now():
            print('Updating api token...')
            api_token = json.loads(requests.get('http://marking.e5.vgg.ru/api/key').content)
            att_token = api_token.get('token_a')
            token_expires = datetime.datetime.strptime(api_token.get('expires'), '%Y-%m-%dT%H:%M:%S.%f')
            print(f'Token updated, new expire datetime is: {token_expires}')
    except Exception as Error:
        print(f'Error updating token: {Error}')
        return False

    h = {'Authorization': f'Bearer {att_token}'}
    req.headers.update(h)
    req.trust_env = False
    return True


def main(debug: int):
    req = requests.Session()
    if update_api_token(req):
        print('Reading task data from database...')
        with pymssql.connect(**read_connection_params()) as conn:
            with conn.cursor(as_dict=True) as cursor:
                cursor.execute('select top 90 guid, ware_gtin from [wares_gtins] where check_result = 0')
                for row in cursor:
                    print(
                        f'Processing api call: https://апи.национальный-каталог.рф/v3/product?gtin={row["ware_gtin"]}')
                    reply = req.get(f'https://апи.национальный-каталог.рф/v3/product?gtin={row["ware_gtin"]}')
                    if reply.status_code == 200:
                        try:
                            if json.loads(reply.content).get('result', '[]')[0].get('good_status', None) == 'published':
                                write_status(row['guid'], 1)
                                print(f'Ware {row["ware_gtin"]} found and its okay.')
                            else:
                                write_status(row['guid'], -2)
                                print(f'Ware {row["ware_gtin"]} found and its in errored state.')
                            try:
                                if debug == 1:
                                    with open(f'{row["guid"]}.json', 'w', encoding='UTF8') as f:
                                        f.write(json.dumps(json.loads(reply.content), indent=4, sort_keys=False,
                                                           ensure_ascii=False, separators=(',', ': ')))
                            except Exception as Err:
                                print(f'Failed save data {Err}')
                        except Exception as ErrG:
                            print(f'Error analyzing data from api: {ErrG}')
                            pass
                    else:
                        if reply.status_code == 429:
                            print(
                                f'Reached queries limit: {reply.reason}, code: {reply.status_code}, '
                                f'next try after: {reply.headers.get("retry-after", "0")} sec.')
                            return int(reply.headers.get("retry-after", 60)) + 5
                        else:
                            write_status(row['guid'], -1)
                            print(f'Ware {row["ware_gtin"]} not found or its in errored state.')
    return 5


if __name__ == "__main__":
    print('Initialization...')
    try:
        save_debug = int(sys.argv[1])
        print('Save debug mode: On')
    except Exception as ParamErr:
        print(f'Param error, waiting 0 or 1, error {ParamErr}')
        save_debug = 0
    while True:
        delay = main(save_debug)
        print(f'Sleeping... {delay} sec.')
        time.sleep(delay)
