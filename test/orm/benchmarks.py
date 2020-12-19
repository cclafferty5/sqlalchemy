import time
import sqlite3
import random
import string
import copy

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Boolean, create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.orm.cache import CachedSessionManager

Base = declarative_base()
DBSession = scoped_session(sessionmaker())
CachedDBSession = CachedSessionManager(sessionmaker())
engine = None
cache_engine = None
TABLESIZE = 30000

class TableOne(Base):
    __tablename__ = "TableOne"
    a = Column(Integer, primary_key=True)
    b = Column(Integer())
    c = Column(Integer())
    d = Column(String(255))
    e = Column(String(255))

class TableTwo(Base):
    __tablename__ = "TableTwo"
    a = Column(Integer, primary_key=True)
    b = Column(Integer())
    c = Column(Integer())
    d = Column(String(255))
    e = Column(String(255))

class TableThree(Base):
    __tablename__ = "TableThree"
    a = Column(Integer, primary_key=True)
    b = Column(Integer())
    c = Column(Integer())
    d = Column(String(255))
    e = Column(String(255))

class TableFour(Base):
    __tablename__ = "TableFour"
    a = Column(Integer, primary_key=True)
    b = Column(Integer())
    c = Column(Integer())
    d = Column(String(255))
    e = Column(String(255))

class TableFive(Base):
    __tablename__ = "TableFive"
    a = Column(Integer, primary_key=True)
    b = Column(Integer())
    c = Column(Integer())
    d = Column(String(255))
    e = Column(String(255))

tables = [TableOne, TableTwo, TableThree, TableFour, TableFive]

def init_sqlalchemy(dbname='sqlite:///sqlalchemy.db'):
    global engine
    global cache_engine
    engine = create_engine(dbname, echo=False)
    cache_engine = create_engine('sqlite:///cache.db', echo=False)
    DBSession.remove()
    DBSession.configure(bind=engine, autoflush=False, expire_on_commit=False)
    CachedDBSession.remove()
    CachedDBSession.configure(bind=cache_engine, autoflush=False, expire_on_commit=False)
    CachedDBSession.cache.reset()
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Base.metadata.drop_all(cache_engine)
    Base.metadata.create_all(cache_engine)

def rand_string(length):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))

def rand_int(low, high):
    return random.randint(low, high)

def create_random_row(T):
    new_row = T()
    new_row.b = rand_int(0,1000)
    new_row.c = rand_int(0,10000)
    new_row.d = rand_string(255)
    new_row.e = rand_string(255)
    return new_row

def fill_table(n, table):
    for i in range(n):
        row1 = create_random_row(table)
        row2 = copy.deepcopy(row1)
        DBSession.add(row1)
        CachedDBSession.add(row2)
        if i % 1000 == 0:
            DBSession.flush()
            CachedDBSession.flush()
    DBSession.commit()
    CachedDBSession.commit()

def fill_tables(n):
    for table in tables:
        print("Creating {} with {} rows".format(table.__tablename__, n))
        fill_table(n, table)

def init_benchmarks(tablesize=TABLESIZE):
    init_sqlalchemy()
    fill_tables(tablesize) 

def random_where_query(table, b, c):
    nc_query = DBSession.query(table).filter(table.b > b, table.c > c)
    c_query = CachedDBSession.query(table).filter(table.b > b, table.c > c)
    return (nc_query, c_query)

def time_queries(queries):
    t0 = time.time()
    for query in queries:
        customers = query.all()
    return time.time() - t0

def time_cached_queries(queries):
    pass

def time_random_queries(num_queries):
    queries = [random_where_query(random.choice(tables), rand_int(0,1000), rand_int(0,10000)) for i in range(num_queries)]
    zipped = list(zip(*queries))
    normal_queries = zipped[0]
    cached_queries = zipped[1]
    no_cache_time, cache_time = time_queries(normal_queries), time_queries(cached_queries)
    hit_rate = CachedDBSession.cache.get_hit_rate()
    print("No Cache time: {}".format(time_queries(normal_queries)))
    print("Cache time: {}".format(time_queries(cached_queries)))
    print("Cache hit rate:", CachedDBSession.cache.get_hit_rate())
    return no_cache_time, cache_time, hit_rate

def run_benchmarks():
    tablesizes = [10000, 20000, 30000, 40000, 50000, 60000]
    results = []
    for tablesize in tablesizes:
        init_benchmarks(tablesize=tablesize)
        no_cache_time, cache_time, hit_rate = time_random_queries(30)
        results.append((no_cache_time, cache_time, hit_rate))
    print(results)

if __name__ == "__main__":
    run_benchmarks()