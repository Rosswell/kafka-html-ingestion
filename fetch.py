import requests
from bs4 import BeautifulSoup as bs
from time import sleep


headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
        'Pragma': 'no-cache'
    }


def fetch_raw(recipe_url):
    html = None
    print('Processing..{}'.format(recipe_url))
    try:
        r = requests.get(recipe_url, headers=headers)
        if r.status_code == 200:
            html = r.text
    except Exception as ex:
        print('Exception while accessing raw html')
        print(str(ex))
    finally:
        return html.strip()


def get_recipes():
    recipes = []
    url = 'https://www.allrecipes.com/recipes/96/salad/'
    print('Accessing list')

    try:
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            html = r.text
            soup = bs(html, 'html.parser')
            links = soup.select('.fixed-recipe-card__h3 a')
            idx = 0
            for link in links:
                sleep(2)
                recipe = fetch_raw(link['href'])
                recipes.append(recipe)
                idx += 1
                if idx > 2:
                    break
    except Exception as ex:
        print('Exception in get_recipes')
        print(str(ex))
    finally:
        return recipes
