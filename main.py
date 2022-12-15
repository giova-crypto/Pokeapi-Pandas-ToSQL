#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import sqlalchemy as sqla
from sqlalchemy.orm import sessionmaker
import requests
import asyncio
import aiohttp



#define dataframes
#
global pokemons
global stats
pokemons = pd.DataFrame(columns=['id', 'name', 'image'])
stats = pd.DataFrame(columns=['pokemon_id', 'stat_id', 'name', 'base_stat'])
types = pd.DataFrame(columns=['pokemon_id', 'type_id', 'name'])
abilities = pd.DataFrame(columns=['pokemon_id', 'ability_id', 'name'])



#define sqlalchemy engine and conn
engine = sqla.create_engine('mysql://root:root@localhost/', echo=False)
conn = engine.connect()
try:
    conn.execute('CREATE DATABASE IF NOT EXISTS tgb')
    conn.close()
except Exception as e:
    print('Error: ', e)
engine = sqla.create_engine('mysql://root:root@localhost/tgb', echo=False)
conn = engine.connect()
print('Database Connection Established')


# # SQL Storing Functions


#Store all data
def sql_insert(dataframe, table):
    dataframe.to_sql(table, con=conn, index=False, if_exists='replace')


# ## Another approach with core API insert
# This is faster than orm save ***add_all*** method and comparable to ***to_sql()*** in case that we want use the orm modeling approach, can use ***bulk_insert_mappings()*** too but is a little bit slower than this



def core_to_sql(dataframe, table):
    rec_list = dataframe.to_dict(orient='records')

    metadata = sqla.schema.MetaData(bind=engine)
    table = sqla.Table(table, metadata, autoload=True)

    Session = sessionmaker(bind=engine)
    session = Session()

    conn.execute(table.insert(), rec_list)
    session.commit()

    session.close()


# # Dataframe populate functions


#Store abilities
def save_stats(st, pokemon_id, st_id):
    global stats
    st_name = st.get("stat").get('name')
    base_stat = st.get('base_stat')
    new_stat = pd.DataFrame(
        {'pokemon_id': pokemon_id, 'stat_id': st_id, 'name': st_name, 'base_stat': base_stat},
        index=[st_id]
    )
    stats = pd.concat([new_stat, stats])



#Store types
def save_types(tp, pokemon_id, tp_id):
    global types
    tp_name = tp.get("type").get('name')
    new_type = pd.DataFrame(
        {'pokemon_id': pokemon_id, 'type_id': tp_id, 'name': tp_name},
        index=[tp_id]
    )
    types = pd.concat([new_type, types])



def save_abilities(ab, pokemon_id, ab_id):
    global abilities
    ab_name = ab.get("ability").get('name')
    new_ability = pd.DataFrame(
        {'pokemon_id': [pokemon_id], 'ability_id': [ab_id], 'name': [ab_name]}
    )
    abilities = pd.concat([new_ability, abilities])


#Create poke
def save_pokemon(data):
    global pokemons
    image = 'https://raw.githubusercontent.com/PokeAPI/sprites/master/sprites/pokemon/' + str(
        data.get('order')) + '.png'
    name = data.get('name')
    st_id = 0
    tp_id = 0
    ab_id = 0
    pokemon_id = data.get('order')
    new_poke = pd.DataFrame({'id': pokemon_id, 'name': name, 'image': image}, index=[pokemon_id])
    pokemons = pd.concat([new_poke, pokemons])
    for st in data.get('stats'):
        st_id += 1
        save_stats(st, pokemon_id, st_id)
    for tp in data.get('types'):
        tp_id += 1
        save_types(tp, pokemon_id, tp_id)
    for ab in data.get('abilities'):
        ab_id += 1
        save_abilities(ab, pokemon_id, ab_id)


# # Requesting API and calling main function


#async methods
async def fetch(s, url):
    async with s.get(url) as r:
        if r.status != 200:
            r.raise_for_status()
        return await r.json()


async def fetch_all(s, urls):
    tasks = []

    for url in urls:
        task = asyncio.create_task(fetch(s, url))
        tasks.append(task)
    res = await asyncio.gather(*tasks)
    return res


async def main():
    req = requests.get('https://pokeapi.co/api/v2/pokemon?limit=100000&offset=0')
    results = req.json().get('results')
    urls = []
    for poke in results:
        url = poke.get('url')
        urls.append(url)

    async with aiohttp.ClientSession() as session:
        pokes = await fetch_all(session, urls)
        for poke in pokes:
            save_pokemon(poke)

        sql_insert(pokemons, 'pokemons')
        sql_insert(stats, 'stats')
        sql_insert(types, 'types')
        sql_insert(abilities, 'abilities')
        print('Data stored successfully')


asyncio.run(main())

#

# # Punto 4
# sql = sqla.text(‘select p.id,p.name, p.image from ptypes as pt join pokemons as p on pt.pokemon_id = p.id group by p.id having COUNT(*) >1 AND p.id > 0;’)
# query = engine.execute(sql)
# p41 = pd.DataFrame(query.fetchall())
# p41.to_csv(“punto4/script1.csv”, index=False)
# print(“CSV punto 4.1 creado”)
#
# sql = sqla.text(‘select pt.name, COUNT() from ptypes as pt group by pt.name order by COUNT() desc limit 1’)
# query = engine.execute(sql)
# p42 = pd.DataFrame(query.fetchall())
# p42.to_csv(“punto4/script2.csv”, index=False)
# print(“CSV punto 4.2 creado”)


sql = sqla.text('select p.id,p.name, p.image from ptypes as pt join pokemons as p on pt.pokemon_id = p.id group by p.id having COUNT(*) >1 AND p.id > 0;')
query = engine.execute(sql)
p41 = pd.DataFrame(query.fetchall())
p41.to_csv("punto4/script1.csv", index=False)
print("CSV punto 4.1 creado")

sql = sqla.text('select pt.name, COUNT(*) from ptypes as pt group by pt.name order by COUNT(*) desc limit 1')
query = engine.execute(sql)
p42 = pd.DataFrame(query.fetchall())
p42.to_csv("punto4/script2.csv", index=False)
print("CSV punto 4.2 creado")

# 

# # Notes
# ### Performance is better than my flask solution:
# [Git Repository of the flask solution](https://github.com/giova-crypto/Pokeapi-Flask-ToSQL)
# 
# This is due to the non-use of the orm methods to store the data and the use of async-await methods that allows me reduce the time to 1/3 of the time compared with my flask solution
# 
# ### Improve performance
# To improve it i need to enhance the way that i save the pokemon data into a dataframe
# 
# # Task state: ***Success***
