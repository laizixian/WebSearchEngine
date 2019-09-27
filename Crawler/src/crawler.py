from googlesearch import search
from queue import PriorityQueue, Empty
from urllib.parse import urlparse
from urllib.parse import urljoin
import urllib.robotparser
from lxml import html
import requests
from concurrent.futures import ThreadPoolExecutor
from threading import RLock
import logging
import time


def extract_root(url):
    """
    get root network location from url
    :param url:
    :return: root url
    """
    parsed_url = urlparse(url)
    root_url = parsed_url.scheme + '://' + parsed_url.netloc
    return root_url


def check_robot(robot_parse, url):
    return robot_parse.can_fetch('*', url)


class MultiThreadCrawler:
    SEED_SIZE = 10
    SEED_SCORE = -100
    lock = RLock()
    logger = logging.getLogger("threaded crawler")
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler("crawler.log")
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    def __init__(self, keywords, worker_num):
        self.keywords = keywords
        self.worker_num = worker_num
        self.priority_queue = PriorityQueue()
        self.robots_rule = {}
        self.crawled_url = set([])
        self.importance_score = {}
        self.novelty = {}
        self.pool = ThreadPoolExecutor(max_workers=worker_num)
        self.crawled_sites = set([])
        self.requested = set([])

    def get_robot(self, root_url):
        """
        get the robot file
        :param root_url:
        :return: RobotFileParser
        """
        new_url = urljoin(root_url, 'robots.txt')
        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(new_url)
        try:
            rp.read()
            return rp
        except Exception as e:
            self.logger.error("robot exception: {}".format(e))
            return None

    def update_importance_score(self, root_url):
        if root_url not in self.importance_score:
            self.importance_score[root_url] = 0
        else:
            self.importance_score[root_url] = self.importance_score[root_url] - 1

    def update_novelty_score(self, root_url):
        if root_url not in self.novelty:
            self.novelty[root_url] = -100
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
                    robot_parser = self.get_robot(root_url)
                    self.robots_rule[root_url] = robot_parser
                self.priority_queue.put((self.SEED_SCORE, (s, 0)))
            except Exception as e:
                self.logger.error("could not add seed: {} with exception: {}".format(s, e))
                pass

    def request_page(self, target_site):
        try:
            url = target_site[1][0]
            root = extract_root(url)
            if root in self.requested:
                time.sleep(5)
                if root in self.requested:
                    self.requested.remove(root)
                res = requests.get(url, timeout=(3, 30))
                self.requested.add(root)
                return res, target_site[0], target_site[1][1]
            else:
                res = requests.get(url, timeout=(3, 30))
                return res, target_site[0], target_site[1][1]
        except requests.RequestException:
            return

    def request_callback(self, res):
        result = res.result()
        if result and result[0].status_code == 200:
            url_result = result[0]
            priority_score = result[1]
            distance = result[2]
            try:
                self.parse_page(url_result, priority_score, distance)
            except Exception as e:
                self.logger.error(e)

    def parse_page(self, request_result, priority_score, distance):
        cur_url = request_result.url
        cur_root_url = extract_root(cur_url)
        self.update_novelty_score(cur_root_url)

        if cur_root_url not in self.robots_rule:
            robot_file = self.get_robot(cur_root_url)
            if robot_file is not None:
                self.robots_rule[cur_root_url] = robot_file
            else:
                raise Exception("can not read robot.txt for {}".format(cur_url))

        if check_robot(self.robots_rule[cur_root_url], cur_url):
            self.logger.info("url: {}, length: {}, distance: {}, priority score: {}".format(cur_url,
                                                                                            len(request_result.text),
                                                                                            distance, priority_score))
            string_doc = None
            try:
                string_doc = html.fromstring(request_result.content)
            except Exception as e:
                # need error logger
                self.logger.error("url : {} , exception: {}".format(cur_url, e))
            if string_doc is not None:
                links = list(string_doc.iterlinks())
                for url in links:
                    if url[1] == 'href':
                        new_url = urljoin(cur_root_url, url[2])
                        new_url_root = extract_root(new_url)
                        if new_url_root != cur_root_url:
                            self.update_importance_score(new_url_root)
                        importance_score = 0
                        novelty_score = -100
                        try:
                            importance_score = self.importance_score[new_url_root]
                            novelty_score = self.novelty[new_url_root]
                        except Exception as e:
                            pass
                        priority_score = importance_score + novelty_score
                        self.priority_queue.put((priority_score, (new_url, distance + 1)))
        else:
            # need info logger
            self.logger.error("can not crawl {} base on robot.txt".format(cur_url))

    def start_crawler_threaded(self):
        self.add_seed()
        while True:
            try:
                target_site = self.priority_queue.get(timeout=5)
                target_url = target_site[1][0]
                if target_url not in self.crawled_url:
                    self.crawled_url.add(target_url)
                    job = self.pool.submit(self.request_page, target_site)
                    job.add_done_callback(self.request_callback)
            except Exception as e:
                self.logger.error(e)
                continue


if __name__ == '__main__':
    key_word = input("enter the key words:")
    worker_number = input("enter the max worker number:")
    crawler = MultiThreadCrawler(key_word, int(worker_number))
    crawler.start_crawler_threaded()
