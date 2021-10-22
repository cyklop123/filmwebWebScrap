import pymongo
import requests
import time, signal
from bs4 import BeautifulSoup

INTERVAL_TIME_SECONDS = 600 # 10 min

def main():
    signal.signal(signal.SIGTERM, programTermination)
    signal.signal(signal.SIGINT, programTermination)

    db = initMongo()
    movies = db['movies']

    while True:
        try:
            print("Starting data collection")
            start = time.time()
            get_data(url = 'https://www.filmweb.pl/serials/search?orderBy=rate&descending=true', db=movies)
            end = time.time()
            print("Finishing data collection")
            print("Elapsed time",end-start)
            print("Next iteration in", INTERVAL_TIME_SECONDS, 'seconds\n')
            time.sleep(INTERVAL_TIME_SECONDS)
        except Termination:
            print("\nProgram terminated.")
            break

def initMongo():
    try:
        client = pymongo.MongoClient("mongodb://minadzd:minadzd@localhost:27017/filmweb")
        db = client['filmweb']
        db.list_collection_names()
        print("Database connection established")
        return db
    except:
        print("Database connection error")
        exit()

def saveData(data, db):
    movie = {
        'filmweb-id': data['id'],
        'name': data['name'],
        'nameOrig': data['nameOrig'],
        'year': data['year'],
        'rate': data['rate'],
        'rateCount': data['rateCount']
    }
    if data['wantToSee']:
        movie['wantToSee'] = data['wantToSee']
    if data['releaseDate']:
        movie['releaseDate'] = data['releaseDate']
    if data['episodeDuration']:
        movie['episodeDuration'] = data['episodeDuration']
    if data['decription']:
        movie['decription'] = data['decription']
    movie['genres'] = list(data['genres'])
    movie['countries'] = list(data['countries'])
    movie['directors'] = list(data['directors'])
    movie['cast'] = list(data['cast'])

    try:
        db.insert_one(movie)
    except:
        print("Movie insertion error")
        exit()

def checkIfFilmExist(id, name, db):
    return True if db.count_documents({'filmweb-id': id, 'name': name}) > 0 else False

def get_data(url, db):
    page=1
    collected = 0
    new = 0
    while True:
        r = requests.get(url+'&page='+str(page))
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, 'html.parser')
            films = soup.find_all("div", class_="filmPreviewHolder")

            if not films: # nie znaleziono filmów => koniec
                print("\nCollected", collected, "films.", new, "new.")
                return

            for film in films:
                data = extractData(film)
                if not checkIfFilmExist(data['id'], data['name'], db):
                    saveData(data, db)
                    new += 1
                collected += 1
        else:
            print("Request error")
            return
        print("\rCollected pages:", page, end='')
        page += 1


def extractData(film):
    id = film.find("div", class_="poster--auto")["data-film-id"].strip()
    name = film.find("h2", class_="filmPreview__title").text.strip()
    nameOrigTag = film.find("div", class_="filmPreview__originalTitle")
    nameOrig = nameOrigTag.text.strip() if nameOrigTag != None else name
    year = film.find("div", class_="filmPreview__year").text.strip()
    rateBoxTag = film.find("div", class_="filmPreview__rateBox")
    rate = rateBoxTag.get('data-rate')
    rateCount = rateBoxTag.get('data-count')
    wantToSeeTag = film.find('div', class_="filmPreview__wantToSee")
    wantToSee = wantToSeeTag.get('data-wanna')
    realeaseDateTag = film.find("div", class_="filmPreview__release")
    releaseDate = realeaseDateTag.get("data-release")
    episodeDurationTag = film.find("div", class_="filmPreview__filmTime")
    episodeDuration = episodeDurationTag.get('data-duration') if episodeDurationTag else None
    descriptionTag = film.find("div", class_="filmPreview__description")
    decription = descriptionTag.text.strip() if descriptionTag else None

    genresTag = film.find("div", class_="filmPreview__info--genres")
    if genresTag:
        genres = (genre.text.strip() for genre in genresTag.find_all('a'))
    else:
        genres = ()
    countriesTag = film.find("div", class_="filmPreview__info--countries")
    if countriesTag:
        countries = (country.text.strip() for country in countriesTag.find_all('a'))
    else:
        countries = ()
    directorsTag = film.find("div", class_="filmPreview__info--directors")
    if directorsTag:
        directors = (director.text.strip() for director in directorsTag.find_all('a'))
    else:
        directors = ()
    castTag = film.find("div", class_="filmPreview__info--cast")
    if castTag:
        cast = (actor.text.strip() for actor in castTag.find_all('a'))
    else:
        cast = ()

    return {
        "id": id,
        "name": name,
        "nameOrig": nameOrig,
        "year": year,
        "rate": rate,
        "rateCount": rateCount,
        "wantToSee": wantToSee,
        "releaseDate": releaseDate,
        "episodeDuration": episodeDuration,
        "decription": decription,
        "genres": genres,
        "countries": countries,
        "directors": directors,
        "cast": cast
    }

class Termination(Exception):
    pass

def programTermination(signum, frame):
    raise Termination

if __name__ == '__main__':
    main()
