import pymongo
import requests
import time, signal
from bs4 import BeautifulSoup

INTERVAL_TIME_SECONDS = 600 # 10 min
PAGE_START = 1
PAGE_END = 10 #docelowo 1000

def main():
    signal.signal(signal.SIGTERM, programTermination)
    signal.signal(signal.SIGINT, programTermination)

    while True:
        try:
            get_data(url = 'https://www.filmweb.pl/serials/search?orderBy=rate&descending=true', pages = range(PAGE_START, PAGE_END+1))
            print("Next iteration in", INTERVAL_TIME_SECONDS, 'seconds\n')
            time.sleep(INTERVAL_TIME_SECONDS)
        except Termination:
            print("\nProgram terminated.")
            break

def saveData(data):
    pass #ma zapisywać pojedynczy film do bazy

def checkIfFilmExist(id):
    return False # ma zwracać true jeżeli film istnieje w bazie lub false jeżeli nie istnieje

def get_data(url, pages):
    collected = 0
    new = 0
    print("Collecting page: 0 /",pages[-1], end='')
    for page in pages:
        r = requests.get(url+'&page='+str(page))
        print("\rCollecting page:", page, '/', pages[-1], end='')
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, 'html.parser')
            films = soup.find_all("div", class_="filmPreviewHolder")

            for film in films:
                data = extractData(film)
                if ~checkIfFilmExist(data[0]):
                    saveData(data)
                    new += 1
                collected += 1
        else:
            break
            print(r.headers)
    print()
    print("Collected", collected, "films.", new, "new.")

def extractData(film):
    id = film.find("div", class_="poster--auto")["data-film-id"]
    name = film.find("h2", class_="filmPreview__title").text
    nameOrigTag = film.find("div", class_="filmPreview__originalTitle")
    nameOrig = nameOrigTag.text if nameOrigTag != None else name
    year = film.find("div", class_="filmPreview__year").text
    rateBoxTag = film.find("div", class_="filmPreview__rateBox")
    rate = rateBoxTag.get('data-rate')
    rateCount = rateBoxTag.get('data-count')
    wantToSeeTag = film.find('div', class_="filmPreview__wantToSee")
    wantToSee = wantToSeeTag.get('data-wanna')
    realeaseDateTag = film.find("div", class_="filmPreview__release")
    realeaseDate = realeaseDateTag.get("data-release")
    episodeDurationTag = film.find("div", class_="filmPreview__filmTime")
    episodeDuration = episodeDurationTag.get('data-duration') if episodeDurationTag else None
    descriptionTag = film.find("div", class_="filmPreview__description")
    decription = descriptionTag.text if descriptionTag else None

    genresTag = film.find("div", class_="filmPreview__info--genres")
    if genresTag:
        genres = [genre.text for genre in genresTag.find_all('a')]
    else:
        genres = []
    countriesTag = film.find("div", class_="filmPreview__info--countries")
    if countriesTag:
        countries = [country.text for country in countriesTag.find_all('a')]
    else:
        countries = []
    directorsTag = film.find("div", class_="filmPreview__info--directors")
    if directorsTag:
        directors = [director.text for director in directorsTag.find_all('a')]
    else:
        directors = []
    castTag = film.find("div", class_="filmPreview__info--cast")
    if castTag:
        cast = [actor.text for actor in castTag.find_all('a')]
    else:
        cast = []

    return [ id, name, nameOrig, year, rate, rateCount, wantToSee, realeaseDate, episodeDuration, decription, genres, countries, directors, cast ]

class Termination(Exception):
    pass

def programTermination(signum, frame):
    raise Termination

if __name__ == '__main__':
    main()
