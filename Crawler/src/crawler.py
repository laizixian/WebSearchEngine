from googlesearch import search
from queue import PriorityQueue
from urllib.parse import urlparse
from urllib.parse import urljoin
import urllib.robotparser
from lxml import html
import requests
from concurrent.futures import ThreadPoolExecutor
from threading import RLock
import logging


def extract_root(url):
    """
    get root network location from url
    :param url:
    :return: root url
    """
    parsed_url = urlparse(url)
    root_url = parsed_url.scheme + '://' + parsed_url.netloc
    return root_url


def get_robot(root_url):
    """
    get the robot file
    :param root_url:
    :return: RobotFileParser
    """
    new_url = urljoin(root_url, 'robots.txt')
    rp = urllib.robotparser.RobotFileParser()
    rp.set_url(new_url)
    rp.read()
    return rp


def check_robot(robot_parse, url):
    return robot_parse.can_fetch('*', url)


def request_page(url):
    try:
        res = requests.get(url, timeout=(3, 30))
        return res
    except requests.RequestException:
        return


class MultiThreadCrawler:
    SEED_SIZE = 10
    SEED_SCORE = -1000
    lock = RLock()

    def __init__(self, keywords, max_hop, worker_num):
        self.keywords = keywords
        self.max_hop = max_hop
        self.worker_num = worker_num
        self.priority_queue = PriorityQueue()
        self.robots_rule = {}
        self.crawled_url = set([])
        self.importance_score = {}
        self.novelty = {}
        self.pool = ThreadPoolExecutor(max_workers=worker_num)
        self.crawled_sites = set([])

    def update_importance_score(self, root_url):
        if root_url not in self.importance_score:
            self.importance_score[root_url] = 0
        else:
            self.importance_score[root_url] = self.importance_score[root_url] - 1

    def update_novelty_score(self, root_url):
        if root_url not in self.novelty:
            self.novelty[root_url] = 0
        else:
            self.novelty[root_url] = self.novelty[root_url] + 2

    def add_seed(self):
        seed_list = search(self.keywords, tld='com', lang='en', num=self.SEED_SIZE, stop=self.SEED_SIZE, pause=1)
        for s in seed_list:
            try:
                root_url = extract_root(s)
                self.update_novelty_score(root_url)
                self.update_importance_score(root_url)
                if root_url not in self.robots_rule:
                    robot_parser = get_robot(root_url)
                    self.robots_rule[root_url] = robot_parser
                self.priority_queue.put((self.SEED_SCORE, s))
            except Exception as e:
                print(e)
                pass

    def request_callback(self, res):
        result = res.result()
        if result and result.status_code == 200:
            self.parse_page(result)

    def parse_page(self, request_result):
        cur_url = request_result.url
        cur_root_url = extract_root(cur_url)
        self.update_novelty_score(cur_root_url)
        self.update_importance_score(cur_root_url)
        print("getting {} {}".format(self.importance_score[cur_root_url] + self.novelty[cur_root_url], cur_url))
        if cur_root_url not in self.robots_rule:
            self.robots_rule[cur_root_url] = get_robot(cur_root_url)
        if check_robot(self.robots_rule[cur_root_url], cur_url):
            try:
                string_doc = html.fromstring(request_result.content)
            except Exception as e:
                print(e, cur_url)
                pass
            links = list(string_doc.iterlinks())
            for url in links:
                if url[1] == 'href':
                    new_url = urljoin(cur_root_url, url[2])
                    new_url_root = extract_root(new_url)
                    if new_url_root != cur_root_url:
                        self.update_importance_score(new_url_root)
                        self.update_novelty_score(new_url_root)
                        priority_score = self.importance_score[new_url_root] + self.novelty[new_url_root]
                        self.priority_queue.put((priority_score, new_url))
                    else:
                        priority_score = self.importance_score[new_url_root] + self.novelty[new_url_root]
                        self.priority_queue.put((priority_score, new_url))
        else:
            print("cant crawl {}".format(cur_root_url))

    def start_crawler(self):
        self.add_seed()
        target_url = self.priority_queue.get(timeout=5)
        target_url = target_url[1]
        self.crawled_url.add(target_url)
        r = request_page(target_url)
        self.parse_page(r)

    def start_crawler_threaded(self):
        self.add_seed()
        for i in range(1, 300):
            #try:
            target_url = self.priority_queue.get(timeout=5)
            score = target_url[0]
            target_url = target_url[1]
            if target_url not in self.crawled_url:
                self.crawled_url.add(target_url)
                job = self.pool.submit(request_page, target_url)
                job.add_done_callback(self.request_callback)
            #except Exception as e:
            #    print(e)
            #    continue


if __name__ == '__main__':
    crawler = MultiThreadCrawler("big data", 1, 100)
    crawler.start_crawler_threaded()

