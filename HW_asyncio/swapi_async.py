import asyncio
import aiohttp
import datetime
import requests

from models import engine, Session, Base, SwapiPeople


async def get_people(page):
    async with aiohttp.ClientSession() as session:
        response = await session.get(f'https://swapi.dev/api/people/?page{page}')
        json_data = await response.json()
        # await session.close()
    return json_data['results']

async def per_data(session, url):
    async with session.get(url) as res:
        data = await res.json()
        return data

async def get_films(session, films):
    data_films = await asyncio.gather(
        *[per_data(session, film_url) for film_url in films]
    )
    return ','.join([data_film['title'] for data_film in data_films])

async def get_species(session, species):
    data_species = await asyncio.gather(
        *[per_data(session, species_url) for species_url in species]
    )
    return ','.join([data_species['name'] for data_species in data_species])

async def get_starships(session, starships):
    data_starships = await asyncio.gather(
        *[per_data(session, starship_url) for starship_url in starships]
    )
    return ','.join([data_starship['name'] for data_starship in data_starships])

async def get_vehicles(session, vehicles):
    data_vehicles = await asyncio.gather(
        *[per_data(session, vehicles_url) for vehicles_url in vehicles]
    )
    return ','.join([data_vehicle['name'] for data_vehicle in data_vehicles])

async def get_homeworld(session, homeworld):
    data_homeworld = await asyncio.gather(
        per_data(session, homeworld)
    )
    return data_homeworld[0]['name']

async def paste_to_db(persons_jsons):
    async with Session() as session:
        objects = []
        async with aiohttp.ClientSession() as http_session:
            for item in persons_jsons:
                films = await get_films(http_session, item['films'])
                species = await get_species(http_session, item['species'])
                starships = await get_starships(http_session, item['starships'])
                vehicles = await get_vehicles(http_session, item['vehicles'])
                homeworld = await get_homeworld(http_session, item['homeworld'])
                object = SwapiPeople(
                    birth_year=item['birth_year'],
                    eye_color=item['eye_color'],
                    films=films,
                    gender=item['gender'],
                    hair_color=item['hair_color'],
                    height=item['height'],
                    homeworld=homeworld,
                    mass=item['mass'],
                    name=item['name'],
                    skin_color=item['skin_color'],
                    species=species,
                    starships=starships,
                    vehicles=vehicles
                )
                objects.append(object)
                print('добавлен герой ', object.name)
                print('из мира ', object.homeworld)
                print('из фильмов ', object.films)

        async with Session() as db_session:
            db_session.add_all(objects)
            await db_session.commit()

async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.create_all)
        await conn.commit()

    pages = requests.get('https://swapi.dev/api/people/').json()['count'] // 10 + 1
    person_coros = [get_people(i) for i in range(1, pages + 1)]
    tasks = []
    for person_coro in person_coros:
        persons = await person_coro
        task = asyncio.create_task(paste_to_db(persons))
        tasks.append(task)
    await asyncio.gather(*tasks)


if __name__ == '__main__':
    start = datetime.datetime.now()
    asyncio.run(main())
    print('время выполнения кода: ', datetime.datetime.now() - start)
