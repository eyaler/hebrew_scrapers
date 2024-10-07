from collections import Counter, defaultdict
from datetime import datetime
import os
import re
import sys
import time

import requests


api_url = 'https://benyehuda.org/api/v1/search'
key = ''  # put your key here or as a CMD argument (get it from: https://benyehuda.org/api_keys/new)
periods = []  # 'ancient', 'medieval', 'enlightenment', 'revival', 'modern', 'no_period'; leave empty for all
file_format = 'txt'  # 'html', 'txt', 'pdf', 'epub', 'mobi', 'docx', 'odt'
skip_exist = True
strip_copyright = True
strip_footer = True
timeout_secs = 60
sleep_secs = 40
throttle_secs = 0
retries = 5
base_folder = 'c:/data/text/heb/benyehuda'
delete_extra_files = True


if sys.argv[1:]:
    key = sys.argv[1]
elif not key:
    print('Usage: python benyehuda_scraper.py API_KEY')
    print('(get key from: https://benyehuda.org/api_keys/new)')
    sys.exit(1)

all_periods = ['modern', 'revival', 'enlightenment', 'medieval', 'ancient', 'no_period']
periods = sorted(periods, key=lambda p: all_periods.index(p)) or all_periods
bad_urls = []
total_dl = 0
total_files = 0
bad_periods = Counter()
all_periods_count = 0
requests_session = requests.Session()
with open('benyehuda_extra_files.txt', 'w') as fextra:
    for period in periods:
        folder = os.path.join(base_folder, file_format, period)
        os.makedirs(folder, exist_ok=True)
        i = 0
        search_after = []
        total_count = None
        files = []
        cnt_new = 0
        cnt_dl = 0
        while not total_count or i < total_count:
            try:
                request_args = dict(file_format=file_format, search_after=search_after, periods=[] if period == 'no_period' else [period])
                time.sleep(throttle_secs)
                response = requests_session.post(api_url, json=dict(request_args, key=key), timeout=timeout_secs)
                assert response, 'status_code=%d (%s)' % (response.status_code, requests.status_codes._codes.get(response.status_code, ['unofficial HTTP error'])[0])
            except Exception as e:
                print(request_args, e)
                time.sleep(sleep_secs)
                continue
            json = response.json()
            if total_count is None:
                total_count = json['total_count']
                effective_total_count = total_count - all_periods_count*(period == 'no_period')
                to_download = max(0, effective_total_count - len(os.listdir(folder)))
                print(f"Fetching period={period} total_count={effective_total_count} to_download={to_download}")
                files = []
            elif total_count != json['total_count']:
                total_count = None
                i = 0
                search_after = []
                continue
            for item in json['data']:
                i += 1
                download_url = item['download_url']
                file = download_url.rsplit('/', 1)[-1]
                item_period = item['metadata']['period']
                if period == 'no_period':
                    if item_period in all_periods and item_period != 'no_period':
                        continue
                    if item_period:
                        bad_periods[item_period] += 1
                files.append(file)
                path = os.path.join(folder, file)
                if skip_exist and os.path.exists(path) and os.path.getsize(path):
                    continue
                cnt_new += 1
                print(f'({total_dl + 1}) new={cnt_new}/{max(cnt_new, to_download)} all={i}/{total_count}:', download_url)
                response = None
                time.sleep(throttle_secs)
                for j in range(retries):
                    try:
                        response = requests_session.get(download_url, timeout=timeout_secs)
                        assert response, 'status_code=%d (%s)' % (response.status_code, requests.status_codes._codes.get(response.status_code, ['unofficial HTTP error'])[0])
                        break
                    except Exception as e:
                        print(f'try {j+1}/{retries}:', e)
                        time.sleep(sleep_secs)
                if not response:
                    bad_urls.append(download_url)
                    continue
                content = response.content
                if file_format == 'txt':
                    if strip_copyright:
                        content = re.sub('.*©.*'.encode(), b'', content).strip() + b'\n'
                    if strip_footer:
                        content = re.sub(b'.*' + 'פרויקט בן־יהודה באינטרנט'.encode() + b'.*', b'', content).strip() + b'\n'
                try:
                    have = False
                    for dup_period in all_periods:
                        if dup_period == period:
                            continue
                        dup_folder = os.path.join(base_folder, file_format, dup_period)
                        if os.path.exists(dup_folder):
                            for dup_file in sorted(os.listdir(dup_folder)):
                                if dup_file == file:
                                    dup_path = os.path.join(dup_folder, file)
                                    with open(dup_path, 'rb') as fdup:
                                        dup_content = fdup.read()
                                    if dup_content == content:
                                        if period == 'no_period':
                                            have = True
                                        else:
                                            os.remove(dup_path)
                                            print('Deleted identical file:', dup_path)
                    if not have:
                        cnt_dl += 1
                        total_dl += 1
                        with open(path, 'wb') as f:
                            f.write(content)
                except KeyboardInterrupt:
                    if os.path.exists(path):
                        os.remove(path)
                    raise
            search_after = json['next_page_search_after']
        all_periods_count += total_count
        folder_files = os.listdir(folder)
        print(f'Downloaded {cnt_dl}/{len(files)} giving {len(folder_files)} files for period={period}')
        total_files += len(folder_files)
        extra_files = sorted(set(folder_files) - set(files))
        if extra_files:
            line = f'Note: found {len(extra_files)} extra files in {folder}: ' + ', '.join(extra_files)
            fextra.write(line + '\n')
            print(line)
            if delete_extra_files:
                deleted = 0
                for file in extra_files:
                    try:
                        os.remove(os.path.join(folder, file))
                        deleted += 1
                    except Exception:
                        pass
                if deleted:
                    if deleted == len(extra_files):
                        line = 'deleted all extra files'
                    else:
                        extra_files = sorted(set(folder_files) - set(files))
                        line = f'{deleted} extra files deleted; {len(extra_files)} extra files remain: ' + ', '.join(extra_files)
                    fextra.write(line + '\n')
                    print(line)
        print()

all_files = defaultdict(list)
for period in all_periods:
    folder = os.path.join(base_folder, file_format, period)
    for file in sorted(os.listdir(folder)):
        all_files[file].append(folder)
dupes = sorted((k, v) for k, v in all_files.items() if len(v) > 1)
with open('benyehuda_dupe_names.txt', 'w') as fdup:
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
            else:
                line += ' (some differ)'
            fdup.write(line + '\n')
            print(line)

if bad_urls:
    print(f'Found {len(bad_urls)} bad URLs:')
    with open('benyehuda_bad_urls.txt', 'a') as f:
        f.write(f'\n{datetime.now()}\n')
        for url in sorted(bad_urls):
            f.write(url + '\n')
            print(url)

if bad_periods:
    print('bad_periods:', bad_periods.most_common())

print(f'Total: downloaded {total_dl} giving {total_files} files for {len(periods)} periods:', periods)
