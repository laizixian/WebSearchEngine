Run the script using:
python3 crawler.py
The program will ask for three user inputs.
First it will ask for the key words of crawling
Second it will ask for the number threads to use(recommend 400)
Third it will ask for the type of crawler, enter BFS or priority

The program will keep running, use ctrl + c to stop the program
the program will generate a log file called crawler.log in the same dir

libraries used for the program:
googlesearch
queue
urllib.parse
urllib.parse
urllib.robotparser
lxml
requests
concurrent.futures
threading
logging
time