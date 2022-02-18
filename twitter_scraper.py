# requires python 3.8+
# pip install git+https://github.com/JustAnotherArchivist/snscrape
# no credentials needed
# expect 200-400k tweets/hour (lower bound from EC2)
# use csv.reader(dialect='excel-tab') to parse (due to quotations and newlines)


import argparse
from collections import Counter
import csv
import os
import re
import time

import snscrape.modules.twitter as sntwitter


parser = argparse.ArgumentParser()
parser.add_argument('--lang', default='he', help='See https://developer.twitter.com/en/docs/twitter-for-websites/supported-languages')
parser.add_argument('--limit', type=int, help='Number of tweets to fetch')
parser.add_argument('--total', type=int, help='Number of tweets of tweets')
parser.add_argument('--delay_min', type=int, default=10, help='Minutes to wait between retries if results stop early')
parser.add_argument('--verbose', type=int, default=500000, help='Number of lines to accumulate between progress printouts')
args = parser.parse_args()
print(args)

headers = ['id', 'datetime', 'username', 'reply_to', 'quote_of', 'replies', 'retweets', 'quotes', 'likes', 'content']

dialect = csv.excel_tab()
dialect.strict = True
latest = None

found = 0
years = Counter()
maxid_arg = ''

with open('tweets_%s.tsv' % args.lang, 'a+', encoding='utf8', newline='') as f:
    f.seek(0)
    for found, row in enumerate(csv.reader(f, dialect=dialect), start=1):
        if found > 1:
            if found == 2:
                latest = row[1]
            years[row[1][:4]] += 1
    writer = csv.writer(f, dialect=dialect)
    if found:
        found -= 1  # account for headers
        assert len(row) == len(headers), (len(row), len(headers))
        maxid_arg = ' max_id:%d' % (int(row[0])-1)
        date = row[1]
        print('found %d tweets. earliest: %s. latest: %s' % (found, date, latest))
    elif not found:
        writer.writerow(headers)
    start = time.time()
    i = 0
    skip = 0
    first = True
    while (not args.limit or i < args.limit) and (not args.total or found + i < args.limit):
        if not first:
            print('Error getting tweets. Will wait %d min. before continuing' % args.delay_min)
            time.sleep(args.delay_min * 60)
            print('Continuing...')
        first = False
        try:
            for tweet in sntwitter.TwitterSearchScraper('lang:' + args.lang + maxid_arg).get_items():
                if tweet.content.endswith((' has been withheld in response to a report from the copyright holder. Learn more.', '\'s account is temporarily unavailable because it violates the Twitter Media Policy. Learn more.')) or args.lang == 'he' and not re.search('[א-ת]', tweet.content):
                    skip += 1
                    continue
                date = str(tweet.date).split('+')[0]
                if not latest:
                    latest = date
                years[row[1][:4]] += 1
                writer.writerow([tweet.id, date, tweet.user.username, tweet.inReplyToTweetId, tweet.quotedTweet.id if tweet.quotedTweet is not None else None, tweet.replyCount, tweet.retweetCount, tweet.quoteCount, tweet.likeCount, tweet.content.replace('\r\n', '\n').replace('\r', '\n').replace('\x00', '')])
                f.flush()
                os.fsync(f.fileno())
                maxid_arg = ' max_id:%d' % (int(tweet.id) - 1)
                i += 1
                if i == args.limit or found + i == args.total:
                    break
                if args.verbose and not i % args.verbose:
                    print('got %d tweets in %.2f hours (skipped: %d). earliest tweet: %s' % (i, (time.time()-start) / 3600, skip, date))
            break
        except Exception as e:
            print(e)
print('got %d tweets in %.2f hours (skipped: %d)' % (i, (time.time()-start) / 3600, skip))
print('total tweets: %d. earliest: %s latest: %s.' % (found + i, date, latest))
print(sorted(years.items()))
