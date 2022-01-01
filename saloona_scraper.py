# you need to download post-sitemap files from http://saloona.co.il/sitemap_index.xml to ./saloona_sitemap


from datetime import datetime
import os
import re
import sys
import time

from bs4 import BeautifulSoup
import cchardet
import lxml
import requests

HebTokenizer = None
sys.path.insert(1, os.path.join('..', 'hebrew_tokenizer'))
try:  # this is only used as a diagnostic
    from hebrew_tokenizer import HebTokenizer
except ImportError:
    pass


cont_after = ''  # optional url
timeout_secs = 60
sleep_secs = 60
sitemap_folder = 'saloona_sitemap'
parser = 'lxml'  # 'html5lib'

cnt = 0
error_404 = 0
error_redirect = 0
skipped = 0
error_parse = 0
error_write = 0
found = not cont_after
prev_url = None
start = time.time()
mode = 'a' if cont_after else 'w'
with open('saloona.txt', mode + 'b') as fsal, open('saloona_bad_final.txt', mode, encoding='utf8') as fbad, open('saloona.log', mode, encoding='utf8') as flog:
    def myprint(*args):
        print(*args)
        print(*args, file=flog, flush=True)

    try:
        for file in sorted(os.listdir(sitemap_folder), key=lambda x: (len(x), x)):
            if not file.startswith('post-sitemap'):
                continue
            myprint(datetime.now().strftime('%Y-%m-%d %H:%M'), file)
            with open(os.path.join(sitemap_folder, file), encoding='utf8') as f:
                urls = re.findall('<loc>(.*)</loc>', f.read())
                for url in urls:
                    if not found:
                        if url == cont_after:
                            found = True
                        continue
                    text = None
                    while text is None:
                        try:
                            with requests.get(url, timeout=timeout_secs) as post:
                                if post.status_code == 404:
                                    myprint('(404 error)', url)
                                    error_404 += 1
                                    break
                                assert post, 'status_code=%d (%s)' % (post.status_code, requests.status_codes._codes.get(post.status_code, ['unofficial HTTP error'])[0])
                                raw = post.content
                                main_soup = BeautifulSoup(raw, parser)
                                soup = main_soup.find('div', itemprop='articleBody')
                                if soup is None:
                                    skipped += 1
                                    #myprint('(skipped)', url)
                                    break
                                try:
                                    new_para_elems = ['address', 'article', 'aside', 'blockquote', 'br', 'dd', 'div',
                                                      'dl', 'dt', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'header',
                                                      'hgroup', 'hr', 'li', 'main', 'ol', 'p', 'pre', 'section', 'ul']
                                    unwanted_elems = ['audio', 'button', 'canvas', 'details', 'dialog', 'figcaption',
                                                      'figure', 'footer', 'form', 'menu', 'nav', 'noscript', 'rdf',
                                                       'summary', 'svg', 'table', 'video']
                                    for item in soup.find_all(new_para_elems + unwanted_elems):
                                        item.insert_before('\n')
                                        item.insert_after('\n')
                                        if item.name in unwanted_elems:
                                            item.replace_with('\n')
                                    for item in soup.select('[style*="display:none"],[style*="display: none"],[style*="visibility:hidden"],[style*="visibility: hidden"]'):
                                        item.clear()
                                    text = post.text.replace('\xa0', ' ').strip()
                                    if text and re.search('[א-ת]', text):
                                        text = re.sub('[\r\n\f\v\u0085\u2028\u2029]+', '\n', text) + '\n\n'
                                        if HebTokenizer and HebTokenizer.find_bad_final(text):
                                            fbad.write(url + '\n' + text)
                                            fbad.flush()
                                            os.fsync(fbad.fileno())
                                        try:
                                            fsal.write(text.encode())
                                            fsal.flush()
                                            os.fsync(fsal.fileno())
                                            cnt += 1
                                        except Exception as e:
                                            myprint(e)
                                            error_write += 1
                                except AssertionError:
                                    raise
                                except Exception as e:
                                    myprint(e)
                                    error_parse += 1
                        except requests.exceptions.TooManyRedirects as e:
                            myprint('(too many redirects error)', url)
                            error_redirect += 1
                            break
                        except Exception as e:
                            myprint(e, url)
                            myprint(datetime.now().strftime('%Y-%m-%d %H:%M'), 'will retry')
                            time.sleep(sleep_secs)
                    prev_url = url
    except KeyboardInterrupt:
        myprint('previous:', prev_url)
        myprint('current: ', url)
        raise
    finally:
        if skipped:
            myprint('skipped: %d' % skipped)
        if error_404:
            myprint('404 errors: %d' % error_404)
        if error_redirect:
            myprint('too many redirects errors: %d' % error_redirect)
        if error_parse:
            myprint('parsing errors: %d' % error_parse)
        if error_write:
            myprint('write errors: %d' % error_write)
        myprint('scraped %d posts in %.1f hours' % (cnt, (time.time()-start) / 3600))
