# based on https://github.com/iTaybb/israblog_backup v2020.06.19


from datetime import datetime
import os
import re
import sys
import time
#import urllib.error

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


ISRABLOG_HOSTNAME = 'http://israblog.co.il'  # 'http://israblog.org'
MIN_BLOG_SCAN = 1
MAX_BLOG_SCAN = 1000000
spam_blogs = [736508, 737273, 749012, 752271, 761179, 805292, 818993, 818994, 840466, 860242, 860773, 860937, 860986, 861114, 861232, 861294, 861482, 861671, 866474]
break_on_blog_with_more_posts_than = 10000
legacy_mode = False
skip_content = False
timeout_secs = 600
sleep_secs = 60
parser = 'lxml'  # 'html5lib'


def get_url(blog_id, postid=None, intent='main', **kwargs):
	if intent == 'main':
		url = "{0}/blogread.asp?blog={1}".format(ISRABLOG_HOSTNAME, blog_id)
	elif intent == 'board_list':
		url = "{0}/board_list.asp?blog={1}".format(ISRABLOG_HOSTNAME, blog_id)
	elif intent == 'posts':
		url = "{0}/blogread.asp?blog={1}&blogcode={2}".format(ISRABLOG_HOSTNAME, blog_id, postid)
	elif intent == 'comments':
		url = "{0}/comments.asp?blog={1}&user={2}".format(ISRABLOG_HOSTNAME, postid, blog_id)
	elif intent == 'sidebar':
		url = "{0}/BlogReadLists.asp?blog={1}".format(ISRABLOG_HOSTNAME, blog_id)
	else:
		raise Exception("Not possible!")

	for k, v in kwargs.items():
		url += "&{}={}".format(k, v)

	return url


def dl_file(src, encoding='windows-1255', ignore=False):
	if not src.startswith('http'):
		src = src.strip('/.')
		src = "{}/{}".format(ISRABLOG_HOSTNAME, src.strip('/'))

	#try:
	with requests.get(src, timeout=timeout_secs) as f:
		assert f, 'status_code=%d (%s)' % (f.status_code, requests.status_codes._codes.get(f.status_code, ['unofficial HTTP error'])[0])
		raw = f.content
		if encoding:
			if ignore:
				raw = raw.decode(encoding, 'ignore')
			else:
				raw = raw.decode(encoding, 'surrogateescape')
	#except urllib.error.URLError as e:
    #	print("URLError while fetching %s: %s" % (src, str(e)))
	#	return ''
	#except UnicodeEncodeError as e:
	#	print("UnicodeEncodeError: %s" % str(e))
	#	return ''

	return raw


def is_blog_exists(blog_id, encoding='windows-1255'):
	while True:
		try:
			url = get_url(blog_id=blog_id)
			raw = dl_file(url, encoding=encoding, ignore=True)
			exists = 'noblog' not in raw
			public = 'private_login' not in raw
			return exists and public
		except Exception as e:
			print(e, url)
			print(datetime.now().strftime('%Y-%m-%d %H:%M'), 'will retry is_blog_exists')
			time.sleep(sleep_secs)


def find_specific_blogs(specific):
	posts_per_blog = {}
	for s in specific:
		s = s.strip(',')
		if not is_blog_exists(s):
			continue
		with open('israblog_%s.txt'%s, 'wb') as f:
			print(s)
			posts, bads = main(blog_id=s, break_on_blog_with_more_posts_than=0)
			if len(posts):
				posts_per_blog[int(s)] = len(posts)
				if not skip_content:
					f.writelines(posts)
					if not legacy_mode:
						f.write(b'\n')
					f.flush()
					os.fsync(f.fileno())
	return len(specific), posts_per_blog


def find_existing_blogs(from_=MIN_BLOG_SCAN, to=MAX_BLOG_SCAN):
	posts_per_blog = {}
	with open('israblog.txt', 'ab') as f, open('israblog_blog_ids.txt', 'a+') as fid, open('israblog_bad.txt', 'a', encoding='utf8') as fbad:
		fid.seek(0)
		blogs = fid.readlines()
		if blogs:
			last = max(from_-1, int(blogs[-1]))
			print('continuing after: %d' % last)
			from_ = last + 1
		for i in range(from_, to + 1):
			s = str(i)
			if i in spam_blogs or not is_blog_exists(s):
				continue
			print(i)
			posts, bads = main(blog_id=s)
			if len(posts):
				posts_per_blog[i] = len(posts)
				if not skip_content:
					f.writelines(posts)
					if not legacy_mode:
						f.write(b'\n')
					f.flush()
					os.fsync(f.fileno())
			fid.write("{}\n".format(i))
			fid.flush()
			os.fsync(fid.fileno())
			fbad.writelines(bads)
			fbad.flush()
			os.fsync(fbad.fileno())
	return to + 1 - from_, posts_per_blog


def main(blog_id, break_on_blog_with_more_posts_than=break_on_blog_with_more_posts_than):
	post_ids = []
	posts = []
	bads = []

	archive_dates = None
	while archive_dates is None:
		try:
			url = get_url(blog_id=blog_id)
			raw = dl_file(url, ignore=True)
			if not raw:
				print(url + '\t@@@ EMPTY BLOG @@@')
				print(raw.strip())
				return [], [url + '\t@@@ EMPTY BLOG @@@\n' + raw.strip() + '\n\n']
			main_soup = BeautifulSoup(raw, parser)
			archive_dates = [x.get('value') for x in main_soup.find('select', id="PeriodsForUser").find_all('option')]
		except Exception as e:
			print(e, url)
			print(datetime.now().strftime('%Y-%m-%d %H:%M'), 'will retry get dates')
			time.sleep(sleep_secs)

	print('dates=%d' % len(archive_dates))
	archive_dates.sort(key=lambda x: x.split('/')[1] + "{:02d}".format(int(x.split('/')[0])), reverse=True)
	for i, date in enumerate(archive_dates):
		pagenum = 1
		pages_count = 1

		while pagenum <= pages_count:
			month, year = date.split('/')
			pages_count = None
			while pages_count is None:
				try:
					url = get_url(blog_id=blog_id, month=month, year=year, pagenum=pagenum)
					raw = dl_file(url, ignore=True)
					soup = BeautifulSoup(raw, parser)
					for tag in soup.find_all('a', href=re.compile('javascript:showCommentsHere')):
						post_id, post_blog_id = tag['href'].split('(')[1].split(')')[0].split(',')
						if blog_id == post_blog_id:
							post_ids.append(post_id)
							if break_on_blog_with_more_posts_than and len(post_ids) > break_on_blog_with_more_posts_than:
								print('too many posts')
								sys.exit(1)
						else:
							print(url + '\t@@@ BOGUS POST ID: ' + post_id + ' @@@')
							bads.append(url + '\t@@@ BOGUS POST ID: ' + post_id + ' @@@\n\n')
					t = soup.find('script', text=re.compile('navigateCount'))
					pages_count = int(t.string.strip().split('=')[1].strip(';')) if t else 1
				except Exception as e:
					print(e, url)
					print(datetime.now().strftime('%Y-%m-%d %H:%M'), 'will retry get postids')
					time.sleep(sleep_secs)

			pagenum += 1

	print('posts=%d' % len(post_ids))
	if skip_content:
		return [b'']*len(post_ids), []
	for i, postid in enumerate(sorted(post_ids, key=int)):
		url = get_url(blog_id=blog_id, intent='posts', postid=postid)
		post = None
		while post is None:
			try:
				raw = dl_file(url)
				soup = BeautifulSoup(raw, parser)
				post = soup.find('td', class_='blog').find('td', class_='blog').find('span', class_='postedit')
				if post is None:
					print(url + '\t@@@ EMPTY POST @@@')
					bads.append(url + '\t@@@ EMPTY POST @@@\n\n')
					break
			except Exception as e:
				print(e, url)
				with open('israblog_error.txt', 'wb') as f:
					try:
						f.write(url.encode() + b'\n')
						f.write(raw.encode('windows-1255', 'surrogateescape'))
					except Exception:
						pass
				print(datetime.now().strftime('%Y-%m-%d %H:%M'), 'will retry get post')
				time.sleep(sleep_secs)
		if post:
			new_para_elems = ['address', 'article', 'aside', 'blockquote', 'br', 'dd', 'div', 'dl', 'dt', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'header', 'hgroup', 'hr', 'li', 'main', 'ol', 'p', 'pre', 'section', 'ul']
			unwanted_elems = ['audio', 'button', 'canvas', 'details', 'dialog', 'figcaption', 'figure', 'footer', 'form', 'menu', 'nav', 'noscript', 'rdf', 'summary', 'svg', 'table', 'video']
			for item in soup.find_all(new_para_elems + unwanted_elems):
				item.insert_before('\n')
				item.insert_after('\n')
				if not legacy_mode and item.name in unwanted_elems:
					item.replace_with('\n')
			if not legacy_mode:
				for item in soup.select('[style*="display:none"],[style*="display: none"],[style*="visibility:hidden"],[style*="visibility: hidden"]'):
					item.clear()
			text = post.text.replace('\xa0', ' ').strip()
			if text and (re.search('[א-ת]', text) or legacy_mode):
				text = re.sub('[\r\n\f\v\u0085\u2028\u2029]+', '\n', text) + '\n\n'
				if HebTokenizer and HebTokenizer.find_bad_final(text):
					bads.append(url + '\n' + text)
				try:
					posts.append(text.encode())
				except Exception as e:
					print(e, url)

	return posts, bads


start = time.time()
specific = sys.argv[1:]
if specific:
	cnt_blogs, posts_per_blog = find_specific_blogs(specific)
else:
	cnt_blogs, posts_per_blog = find_existing_blogs()
cnt_posts = sum(posts_per_blog.values())
print('largest blogs:')
print(sorted(posts_per_blog.items(), key=lambda x: (x[1], x[0])[-30:]))
print('avg_posts_per_blog=%.1f populated_blog_frac=%.2f' % (cnt_posts/len(posts_per_blog), len(posts_per_blog)/cnt_blogs))
print('scraped %d posts from %d non-empty blogs (out of %d blog ids) in %.1f hours' % (cnt_posts, len(posts_per_blog), cnt_blogs, (time.time()-start) / 3600))
