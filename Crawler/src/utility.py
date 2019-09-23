from bs4 import BeautifulSoup
import requests
import urllib.robotparser
from urllib.parse import urlparse
from urllib.parse import urljoin
from lxml import html


def parse_link(url):
    """
    use lxml library to parse links from html
    :param url:
    :return: a list of links
    """
    r = requests.get(url=url)
    string_doc = html.fromstring(r.content)
    links = list(string_doc.iterlinks())
    root_url = extract_root(url)
    print(r.status_code)
    print(r.url)
    url_list = []
    for i in links:
        if i[1] == 'href':
            new_url = urljoin(root_url, i[2])
            url_list.append(new_url)
    return url_list
    #print(r.content)




def extract_root(url):
    parsed_url = urlparse(url)
    root_url = parsed_url.scheme + '://' + parsed_url.netloc
    return root_url


def check_robot(url):
    """
    check if can craw the url base on robot.txt
    :param url:
    :return: boolean
    """
    robot_url = urljoin(extract_root(url), 'robots.txt')
    print(robot_url)
    rp = urllib.robotparser.RobotFileParser()
    print(robot_url)
    rp.set_url(robot_url)
    print(robot_url)
    rp.read()
    print(rp)
    return rp.can_fetch('*', url)


def crawl_page(url):
    """
    crawl the page base on url return the a list of hyperlinks in the page
    :param url: String
    :return: List of String
    """
    r = requests.get(url=url)
    soup = BeautifulSoup(r.content, 'html5lib')
    root_url = extract_root(url)
    url_list = []
    for link in soup.findAll("a"):
        path = link.get('href')
        new_url = urljoin(root_url, path)
        if check_robot(new_url):
            url_list.append(new_url)
    return url_list


def add_to_dir(dir):
    dir['12'] = 12


if __name__ == '__main__':
    #dir = {}
    #add_to_dir(dir)
    #print(dir)
    #print(crawl_page('https://cnn.com/entertainment'))
    #print(check_robot('https://plus.google.com/113309602424331063294'))
    #print(parse_link('https://cnn.com/entertainment'))
    check_robot('https://www.bestbuy.com')
