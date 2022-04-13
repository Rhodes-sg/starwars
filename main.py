from collection import Collection


characters = Collection('starwars', 'characters')
starships = Collection('starwars', 'starships')

starships.api_scrape("https://swapi.dev/api/starships")

starships.reference(characters, 'pilots')

starships.insert_collection()
