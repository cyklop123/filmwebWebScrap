import pymongo
import requests
import time, signal
from bs4 import BeautifulSoup
# zmienna okreslajaca czas do kolejnego wykonania programu
INTERVAL_TIME_SECONDS = 60*60*24 # 1 raz dziennie
# glowna funkcja programu
def main():
    signal.signal(signal.SIGTERM, programTermination)
    signal.signal(signal.SIGINT, programTermination)
# inicjowanie baz danych, lokalnej i zdalnej
    db = initMongo("mongodb://minadzd:minadzd@localhost:27017/admin")
    db_remote = initMongo("mongodb+srv://user:pRSb39SB3Y5vxRz@cluster0.oezfn.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
    movies = db['movies']
    movies_remote = db_remote['movies']
# glowna petla programu
    while True:
        try:
            print("Starting data collection")
            start = time.time()
            # pobranie danych z podanej strony
            for orderBy in ["rate", "count", "year", "w2s", "popularity"]: # uwzględnienie wszystkich możliwych opcji
                print("Collecting by: "+orderBy+", descending")
                get_data(url = 'https://www.filmweb.pl/serials/search?orderBy='+orderBy+'&descending=true', db=movies, db2=movies_remote)
                print("Collecting by: "+orderBy+", ascending")
                get_data(url = 'https://www.filmweb.pl/serials/search?orderBy='+orderBy+'&descending=false', db=movies, db2=movies_remote)
            end = time.time()
            print("Finishing data collection")
            print("Elapsed time",end-start)
            print("Next iteration in", INTERVAL_TIME_SECONDS, 'seconds\n')
            time.sleep(INTERVAL_TIME_SECONDS)
        except Termination:
            print("\nProgram terminated.")
            break
# funkcja inicjujaca polaczenie z mongoDB
def initMongo(url):
    try:
        client = pymongo.MongoClient(url)
        db = client['filmweb']
        db.list_collection_names()
        print("Database connection established:",url)
        return db
    except:
        print("Database connection error")
        exit()
# funkcja zapisujaca dane do bazy danych
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
# funkcja sprawdzajaca czy w bazie danych znajduje sie juz serial o danym id i nazwie
def checkIfFilmExist(id, name, db):
    return True if db.count_documents({'filmweb-id': id, 'name': name}) > 0 else False
# funkcja pobierajaca dane z podanej strony
def get_data(url, db, db2):
    page=1
    collected = 0
    new = 0
    while True:
        # polaczenie z podana strona
        r = requests.get(url+'&page='+str(page))
        # sprawdzenie statusu polaczenia, jezeli 200 to wykonujemy ekstrakcje danych
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, 'html.parser')
            films = soup.find_all("div", class_="filmPreviewHolder")

            if not films: # nie znaleziono filmów => koniec
                print("\nCollected", collected, "films.", new, "new.")
                return

            for film in films:
                # wydobycie z drzewa DOM danych dotyczacych serialu
                data = extractData(film)
                # sprawdzenie czy serial o danym id lub nazwie juz jest w bazie danych
                # jesli tak to nie zapisujemy go ponownie
                if not checkIfFilmExist(data['id'], data['name'], db):
                    # zapis danych do bazy danych
                    saveData(data, db)
                    new += 1
                # sprawdzenie czy serial o danym id lub nazwie juz jest w bazie danych
                # jesli tak to nie zapisujemy go ponownie (baza zdalna)
                if not checkIfFilmExist(data['id'], data['name'], db2):
                    # zapis danych do bazy danych
                    saveData(data, db2)
                collected += 1
        else:
            print("Request error")
            return
        print("\rCollected pages:", page, end='')
        page += 1

# funkcja odpowiedzialna za przypisanie pobranych danych do konkretnych zmiennych
# nastepnie zwraca nowy obiekt z tymi danymi
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
