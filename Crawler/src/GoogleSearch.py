from googlesearch import search


def get_search_result(query, result_num):
    url_list = search(query, tld='com', lang='en', num=result_num, stop=result_num, pause=1)
    return url_list


if __name__ == '__main__':
    result = get_search_result('what if', 10)
    for i in result:
        print(i)

