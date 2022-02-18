from collections import defaultdict
import os
import re
import requests
import sys
import time


api_url = 'https://benyehuda.org/api/v1/search'
key = ''  # put your key here or as a CMD argument (get it from: https://benyehuda.org/api_keys/new)
periods = []  # 'ancient', 'medieval', 'enlightenment', 'revival', 'modern', 'no_period'; leave empty for all
file_format = 'txt'  # 'html', 'txt', 'pdf', 'epub', 'mobi', 'docx', 'odt'
skip_exist = True
strip_copyright = True
strip_footer = True
timeout_secs = 60
sleep_secs = 10
retries = 3
base_folder = 'd:/data/text/heb/benyehuda'

if len(sys.argv) > 1:
    key = sys.argv[1]
elif not key:
    print('Usage: python benyehuda_scraper.py API_KEY')
    print('(get key from: https://benyehuda.org/api_keys/new)')
    sys.exit(1)

all_periods = ['modern', 'revival', 'enlightenment', 'medieval', 'ancient', 'no_period']
periods = periods or all_periods
bad_urls = []
all_files = defaultdict(list)
with open('benyehuda_extra_files.txt', 'w') as fextra:
    for period in periods:
        folder = os.path.join(base_folder, file_format, period)
        os.makedirs(folder, exist_ok=True)
        i = 0
        page = 1
        total_count = None
        files = []
        cnt_dl = 0
        while not total_count or i < total_count:
            try:
                request_args = dict(file_format=file_format, page=page, periods=[period])
                if period == 'no_period':
                    del request_args['periods']
                response = requests.post(api_url, json=dict(request_args, key=key), timeout=timeout_secs)
                assert response, 'status_code=%d (%s)' % (response.status_code, requests.status_codes._codes.get(response.status_code, ['unofficial HTTP error'])[0])
            except Exception as e:
                print(request_args, e)
                time.sleep(sleep_secs)
                continue
            json = response.json()
            if total_count is None:
                total_count = json['total_count']
                print(f'Fetching period={period} total_count={total_count}')
            elif total_count != json['total_count']:
                i = 0
                page = 1
                total_count = None
                files = []
                cnt_dl = 0
                continue
            for item in json['data']:
                i += 1
                download_url = item['download_url']
                file = download_url.rsplit('/', 1)[-1]
                item_period = item['metadata']['period']
                if period == 'no_period':
                    if item_period in periods:
                        continue
                files.append(file)
                path = os.path.join(folder, file)
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
                    continue
                cnt_dl += 1
                content = response.content
                if file_format == 'txt':
                    if strip_copyright:
                        content = re.sub('.*©.*'.encode(), b'', content).strip() + b'\n'
                    if strip_footer:
                        content = re.sub(b'.*' + 'פרויקט בן־יהודה באינטרנט'.encode() + b'.*', b'', content).strip() + b'\n'
                try:
                    have = False
                    for dup_period in all_periods:
                        dup_folder = os.path.join(base_folder, file_format, dup_period)
                        for dup_file in os.listdir(dup_folder):
                            if dup_file == file:
                                dup_path = os.path.join(dup_folder, file)
                                with open(dup_path, 'rb') as fdup:
                                    dup_content = fdup.read()
                                if dup_content == content:
                                    if period == 'no_period':
                                        have = True
                                    else:
                                        os.remove(dup_path)
                                        print(f'Deleted identical file: {dup_path}')
                    if not have:
                        with open(path, 'wb') as f:
                            f.write(content)
                except KeyboardInterrupt:
                    if os.path.exists(path):
                        os.remove(path)
                    raise
            page += 1
        print(f'Downloaded {cnt_dl}/{len(files)} files for period={period}')
        folder_files = os.listdir(folder)
        extra_files = set(folder_files) - set(files)
        if extra_files:
            line = f'Note: found {len(extra_files)} extra files in {folder}:' + ', '.join(extra_files)
            fextra.write(line + '\n')
            print(line)
        for file in folder_files:
            all_files[file].append(folder)
        print()

with open('benyehuda_dupe_names.txt', 'w') as fdup:
    dupes = sorted((k, v) for k, v in all_files.items() if len(v) > 1)
    if dupes:
        print(f'Found {len(dupes)} duplicate names:')
        for file, folders in dupes:
            line = f'{file} ({len(folders)}): ' + ', '.join(sorted(folders))
            contents = []
            for folder in folders:
                with open(os.path.join(folder, file), 'rb') as f:
                    contents.append(f.read())
            if all(content == contents[0] for content in contents[1:]):
                line += ' (all identical)'
            fdup.write(line + '\n')
            print(line)

if bad_urls:
    print(f'Found {len(bad_urls)} bad URLs:')
    with open('benyehuda_bad_urls.txt', 'a') as f:
        for url in sorted(bad_urls):
            f.write(url + '\n')
            print(url)
