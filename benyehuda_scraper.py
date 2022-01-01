import os
import re
import requests
import sys
import time


api_url = 'https://benyehuda.org/api/v1/search'
key = ''  # put your key here or as a CMD argument (get it from: https://benyehuda.org/api_keys/new)
periods = []  # 'ancient', 'medieval', 'enlightenment', 'revival', 'modern'; leave empty for all
file_format = 'txt'  # 'html', 'txt', 'pdf', 'epub', 'mobi', 'docx', 'odt'
skip_exist = True
strip_footer = True
timeout_secs = 60
sleep_secs = 10
retries = 3

if len(sys.argv) > 1:
    key = sys.argv[1]
elif not key:
    print('Usage: python benyehuda_scraper.py API_KEY')
    print('(get key from: https://benyehuda.org/api_keys/new)')
    sys.exit(1)
periods = periods or ['modern', 'revival', 'enlightenment', 'medieval', 'ancient']
bad_urls = []
with open('ben_yehuda_bad_urls.txt', 'a') as fbad:
    for period in periods:
        folder = os.path.join(file_format, period)
        os.makedirs(folder, exist_ok=True)
        i = 0
        page = 1
        total_count = None
        while not total_count or i < total_count:
            try:
                request_args = dict(file_format=file_format, page=page, periods=[period])
                response = requests.post(api_url, json=dict(request_args, key=key), timeout=timeout_secs)
                assert response, 'status_code=%d (%s)' % (response.status_code, requests.status_codes._codes.get(response.status_code, ['unofficial HTTP error'])[0])
            except Exception as e:
                print(request_args, e)
                time.sleep(sleep_secs)
                continue
            json = response.json()
            if total_count is None:
                total_count = json['total_count']
            elif total_count != json['total_count']:
                i = 0
                page = 1
                continue
            for item in json['data']:
                i += 1
                download_url = item['download_url']
                path = os.path.join(folder, download_url.rsplit('/', 1)[-1])
                if skip_exist and os.path.exists(path) and os.path.getsize(path):
                    continue
                print(f'{i}/{total_count}:', download_url)
                response = None
                for j in range(retries):
                    try:
                        response = requests.get(download_url, timeout=timeout_secs)
                        assert response, 'status_code=%d (%s)' % (response.status_code, requests.status_codes._codes.get(response.status_code, ['unofficial HTTP error'])[0])
                    except Exception as e:
                        print(f'try #{j+1}/{retries}:', e)
                if not response:
                    bad_urls.append(download_url)
                    fbad.write(download_url + '\n')
                    fbad.flush()
                    continue
                content = response.content
                if strip_footer and file_format == 'txt':
                    content = re.sub(b'.*' + 'פרויקט בן־יהודה באינטרנט'.encode('utf8') + b'.*', b'', content).strip()
                try:
                    with open(path, 'wb') as f:
                        f.write(content)
                except KeyboardInterrupt:
                    if os.path.exists(path):
                        os.remove(path)
                    raise
            page += 1
        print(f'Got {i}/{total_count} files for period={period}')
        if len(os.listdir(folder)) != total_count:
            print(f'Note: different number of {len(os.listdir(folder))} files found in folder')
        print()
if bad_urls:
    print(f'Found {len(bad_urls)} bad URLs')
    for url in bad_urls:
        print(url)
