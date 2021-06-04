
import argparse
import json
from datetime import datetime, timedelta
from functools import reduce
from math import ceil
from os import path
from time import sleep
import tweepy
import pandas as pd
from authentication import authentication
from datetime import datetime

import tweepy

from requests import get, codes
from requests_oauthlib import OAuth1
from selenium import webdriver
from re import findall, IGNORECASE

######################### METADATA ATTRIBUTES ##########################
# created_at, id, id_str, full_text, truncated, display_text_range,
# entities, source, in_reply_to_status_id, in_reply_to_status_id_str,
# in_reply_to_user_id, in_reply_to_user_id_str, in_reply_to_screen_name,
# user, geo, coordinates, place, contributors, is_quote_status,
# retweet_count, favorite_count, favorited, retweeted, lang
########################################################################
# Scraping Variables
METADATA_LIST = [  # note: "id" is automatically included (hash key)
    "full_text",
    "id",
    "created_at",
    "user",
    "retweet_count",
    "favorite_count",
    "source",
    "is_quote_status",
    "entities",
    "in_reply_to_screen_name",
    "in_reply_to_status_id",
    "in_reply_to_user_id"
]

# Scraping Variables
TWEET_TAG = "article"

# CONSTANTS
DATE_FORMAT = "%Y-%m-%d"
BATCH_SIZE = 100  # http://docs.tweepy.org/en/v3.5.0/api.html#API.statuses_lookup
API_DELAY = 6  # seconds
TWEET_LIMIT = 3200  # recent tweets
API_LIMIT = 200  # tweets at once

# OUTPUT COLORS
#RESET = "\033!!"
#bw = lambda s: str(s) + RESET  # bold white
#w = lambda s: str(s) + RESET  # white
#g = lambda s: str(s) + RESET  # green
#y = lambda s: str(s) + RESET  # yellow


class Scraper:

    def __init__(self, handle):
        self.api = self.__authorize()
        self.handle = handle.lower()
        self.outfile = self.handle + ".json"
        self.new_tweets = set()  # ids
        self.tweets = self.__retrieve_existing()  # actual tweets

    @staticmethod
    def __authorize():
        consumer_key = "Y84UvX4ps3yblHzesIMM3oRLy"
        consumer_secret = "mI0JXmYrov84jKl7iiqepuxQnHpWwt158dv8lUCOW0Mc0vDfrZ"
        access_token = "873867380-ByXB7sWO6fG6lIAjaASnX0pibYgxvBsRnuFUCe8w"
        access_token_secret = "SV8fr34dnkXFGc2eKBFwxNkzGSCBhiCFvC1zW3DP1IIHn"

        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        api = tweepy.API(auth)
        return api

    def __retrieve_existing(self):
        tweets = dict()
        if path.exists(self.outfile):
            print("ya existen los datos de este usuario")

        return tweets

    def __check_if_scrapable(self):
        try:
            u = self.api.get_user(self.handle)
            if not u.following and u.protected:
                exit("No se puede scrapear un usuario a no ser que este usuario lo esté siguiendo en Twitter.")
        except tweepy.TweepError as e:
            if e.api_code == 50:
                exit("Este usuario no existe.")
            else:
                raise e

    def __can_quickscrape(self):
        usr = self.api.get_user(self.handle)
        return usr.statuses_count <= TWEET_LIMIT

    def scrape(self, start, end, by, loading_delay):
        self.__check_if_scrapable()
        pprint("Scrapeando del usuario @" + self.handle + "...")
        pprint(str(len(self.tweets)) + " tweets existentes en " + self.outfile)
        pprint("Buscando tweets...")
        if self.__can_quickscrape():
            pprint("El usuario tiene menos de " + str(TWEET_LIMIT) + " tweets, hacemos quickscrape...")
            self.__quickscrape(start, end)
        else:
            self.__find_tweets(start, end, by, loading_delay)
            pprint("Encontrados" + str(len(self.new_tweets)) + "nuevos tweets")
            pprint("Devolviendo tweets nuevos tiempo estimado: " + str(ceil(len(self.new_tweets) / BATCH_SIZE) * API_DELAY) + " segundos...")
            self.__retrieve_new_tweets()
        pprint("Scrape finalizado")

    def __quickscrape(self, start, end):
        # can't use Tweepy, need to call actual API
        def authorize():
            return OAuth1("Y84UvX4ps3yblHzesIMM3oRLy", "mI0JXmYrov84jKl7iiqepuxQnHpWwt158dv8lUCOW0Mc0vDfrZ", "873867380-ByXB7sWO6fG6lIAjaASnX0pibYgxvBsRnuFUCe8w", "SV8fr34dnkXFGc2eKBFwxNkzGSCBhiCFvC1zW3DP1IIHn")

        def form_initial_query():
            base_url = "https://api.twitter.com/1.1/statuses/user_timeline.json"
            query = "?screen_name={}&count={}&tweet_mode=extended".format(self.handle, API_LIMIT)
            return base_url + query

        def form_subsequent_query(max_id):
            base_url = "https://api.twitter.com/1.1/statuses/user_timeline.json"  # don't use the is_retweet field!
            query = "?screen_name={}&count={}&tweet_mode=extended&max_id={}".format(self.handle, API_LIMIT, max_id)
            return base_url + query

        def make_request(query):
            request = get(query, auth=authorize())
            if request.status_code == codes.ok:
                return dict((tw["id_str"], tw) for tw in request.json())
            else:
                request.raise_for_status()

        def retrieve_payload():
            recent_payload = make_request(form_initial_query())  # query initial 200 tweets
            all_tweets = dict(recent_payload)
            for _ in range(ceil(TWEET_LIMIT / API_LIMIT) - 1):  # retrieve the other 3000 tweets
                oldest_tweet = list(recent_payload.keys())[-1]  # most recently added tweet is oldest
                recent_payload = make_request(form_subsequent_query(max_id=oldest_tweet))
                all_tweets.update(recent_payload)
            return all_tweets

        def filter_tweets(tweets):
            def get_date(tw):  # parse the timestamp as a datetime and remove timezone
                return datetime.strptime(tw[1]["created_at"], "%a %b %d %H:%M:%S %z %Y").replace(tzinfo=None)

            return dict(filter(lambda tweet: start <= get_date(tweet) <= end, tweets.items()))

        def extract_metadata(tweets):
            return dict((id, {attr: tw[attr] for attr in METADATA_LIST}) for tw in tweets.items())

        new_tweets = extract_metadata(filter_tweets(retrieve_payload()))
        pprint("Encontrados" + str(len(new_tweets.keys() - self.tweets.keys())) + " nuevos tweets")
        self.tweets.update(new_tweets)

    def __find_tweets(self, start, end, by, delay):

        def slide(date, i):#función cambio fecha
            return date + timedelta(days=i)
        def form_url(start, end): #petición
            base_url = "https://twitter.com/search"
            query = "?f=tweets&vertical=default&q=from%3A{}%20since%3A{}%20until%3A{}include%3Aretweets&src=typd"
        if self.__retrieve_existing():
            repliest = []
            with open(self.outfile) as o:
                tweets = json.load(o)

                for j in range(len(tweets['id'])):
                    ids_tweets = tweets['id']
                    base_url = 'https://twitter.com/search?f=tweets&vertical=default&q=to%3A'
                    query= user + '%20since_id%3A' + ids_tweets[j] + 'include%3Aretweets&src=typd'

            return base_url + query.format(self.handle, start, end)

        def parse_tweet_ids():
            return set(findall(f'(?<="/{self.handle}/status/)[0-9]+', driver.page_source, flags=IGNORECASE))

        with init_chromedriver(debug=False) as driver:#más opciones: Chrome(),Firefox()...
            days = (end - start).days + 1
            #scrapeamos tweets con una ventana
            window_start, ids = start, set()
            for _ in range(days)[::by]:
                #scrapeamos la ventana de tiempo adecuada para los tweets
                #hay que formatear la ventana al principio y al final
                since = window_start.strftime(DATE_FORMAT)
                until = slide(window_start, by).strftime(DATE_FORMAT)
                #consultamos tweets
                driver.get(form_url(since, until))
                window_start = slide(window_start, by)
                #cargamos
                sleep(delay)
                #nos movemos en la página hasta que no haya más tweets
                first_pass, second_pass = [-1], []
                while first_pass != second_pass:
                    first_pass = parse_tweet_ids()
                    ids |= first_pass
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    sleep(delay)
                    second_pass = parse_tweet_ids()
                    ids |= second_pass
                if len(second_pass) == 0:
                    print("> No hay tweets en el periodo {} -- {}".format(since, until))
                print("Tweets encontrados hasta ahora: " + str(len(ids)))
            # elimina tweets conocidos de tweets recién encontrados
            self.new_tweets = ids - self.tweets.keys()

    def __retrieve_new_tweets(self):
        tweets = self.__collect_new_tweet_metadata()
        self.tweets.update(tweets)
        self.new_tweets = set()  #reset

    def __collect_new_tweet_metadata(self):
        batch_indices = range(0, len(self.new_tweets), BATCH_SIZE)
        new_tweet_list = list(self.new_tweets)
        batches = [new_tweet_list[i:i + BATCH_SIZE] for i in batch_indices]
        batch_num = 0  #enumeración

        def extract_data(tweet_list):  # diccionario para id: pares de metadatos
            return dict((tw["id"], {attr: tw[attr] for attr in METADATA_LIST}) for tw in tweet_list)

        def staggered_lookup(id_batch):
            nonlocal batch_num
            batch_num += 1
            print("-" + "batch %s of %s" % (batch_num, len(batches)))
            queried_tweets = self.api.statuses_lookup(id_batch, tweet_mode="extended")
            sleep(API_DELAY)
            return extract_data(t._json for t in queried_tweets)
        #recogemos los tweets como una lista de diccionarios
        tweet_batches = [staggered_lookup(batch) for batch in batches]
        def dict_combiner(d1, d2): return d1.update(d2) or d1
        #los juntamos
        tweets = reduce(dict_combiner, tweet_batches) if len(tweet_batches) != 0 else dict()
        return tweets
    def dump_tweets(self):#los guardamos
        with open(self.outfile, "w") as o:
            json.dump(self.tweets, o, indent=4)
        pprint("Tweets guardados en " + self.outfile)

def get_join_date(handle):
    baby_scraper = Scraper(handle)
    join_date = baby_scraper.api.get_user(handle).created_at
    return join_date

def pprint(*arguments):
    print("[", *arguments, "]")

def init_chromedriver(debug=False):
    options = webdriver.ChromeOptions()
    if not debug:
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument("--disable-setuid-sandbox")
    return webdriver.Chrome(options=options)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="scrape.py", usage="python3 %(prog)s [options]",
                                     description="scrape.py - Twitter Scraping Tool")
    parser.add_argument("-u", "--username", help="Scrape this user's Tweets", required=True)
    parser.add_argument("--since", help="Get Tweets after this date (Example: 2010-01-01).")
    parser.add_argument("--until", help="Get Tweets before this date (Example: 2018-12-07).")
    parser.add_argument("--by", help="Scrape this many days at a time", type=int, default=7)
    parser.add_argument("--delay", help="Time given to load a page before scraping it (seconds)", type=int, default=3)
    args = parser.parse_args()

    begin = datetime.strptime(args.since, DATE_FORMAT) if args.since else get_join_date(args.username)
    end = datetime.strptime(args.until, DATE_FORMAT) if args.until else datetime.now()

    user = Scraper(args.username)
    user.scrape(begin, end, args.by, args.delay)
    user.dump_tweets()