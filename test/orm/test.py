import time
import sqlite3

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String,  create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.orm.cache import CachedSessionManager

Base = declarative_base()
DBSession = scoped_session(sessionmaker())
CachedDBSession = CachedSessionManager(sessionmaker())
engine = None
cache_engine = None


class Customer(Base):
    __tablename__ = "customer"
    id = Column(Integer, primary_key=True)
    name = Column(String(255))


def init_sqlalchemy(dbname='sqlite:///sqlalchemy.db'):
    global engine
    engine = create_engine(dbname, echo=False)
    cache_engine = create_engine('sqlite:///cache.db', echo=False)
    DBSession.remove()
    DBSession.configure(bind=engine, autoflush=False, expire_on_commit=False)
    CachedDBSession.remove()
    CachedDBSession.configure(bind=cache_engine, autoflush=False, expire_on_commit=False)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Base.metadata.drop_all(cache_engine)
    Base.metadata.create_all(cache_engine)



def test_sqlalchemy_cache():
    init_sqlalchemy()
    customer = Customer()
    customer.name = 'NAME 1'
    CachedDBSession.add(customer)
    CachedDBSession.commit()
    customer1 = CachedDBSession.query(Customer).filter_by(name='NAME 1').one()
    customer1_cached = CachedDBSession.query(Customer).filter_by(name='NAME 1').one() # should be cached this time
    print("Cache is correct: ", customer1 is customer1_cached)
    CachedDBSession.remove()
    customer1_again = CachedDBSession.query(Customer).filter_by(name='NAME 1').one()
    print("Cache is correct even in a new session:", customer1_again is customer1)

def test_sqlalchemy_cache_bulk(n=100000):
    init_sqlalchemy()
    for i in range(n):
        customer1 = Customer()
        customer2 = Customer()
        customer1.name = customer2.name = 'NAME 1'
        DBSession.add(customer1)
        CachedDBSession.add(customer2)
        if i % 1000 == 0:
            DBSession.flush()
            CachedDBSession.flush()
    DBSession.commit()
    CachedDBSession.commit()
    t0 = time.time()
    customers = DBSession.query(Customer).filter_by(name='NAME 1').all()
    no_cache_time = time.time() - t0
    t0 = time.time()
    cached_customers = CachedDBSession.query(Customer).filter_by(name='NAME 1').all()
    cache_time = time.time() - t0
    print("No cache time:", no_cache_time)
    print("Cached time:", cache_time)



def test_sqlalchemy_orm(n=100000):
    init_sqlalchemy()
    t0 = time.time()
    for i in range(n):
        customer = Customer()
        customer.name = 'NAME ' + str(i)
        DBSession.add(customer)
        if i % 1000 == 0:
            DBSession.flush()
    DBSession.commit()
    print(
        "SQLAlchemy ORM: Total time for " + str(n) +
        " records " + str(time.time() - t0) + " secs")


def test_sqlalchemy_orm_pk_given(n=100000):
    init_sqlalchemy()
    t0 = time.time()
    for i in range(n):
        customer = Customer(id=i + 1, name="NAME " + str(i))
        DBSession.add(customer)
        if i % 1000 == 0:
            DBSession.flush()
    DBSession.commit()
    print(
        "SQLAlchemy ORM given primary key: Total time for " + str(n) +
        " records " + str(time.time() - t0) + " secs")


def test_sqlalchemy_orm_bulk_save_objects(n=100000):
    init_sqlalchemy()
    t0 = time.time()
    for chunk in range(0, n, 10000):
        DBSession.bulk_save_objects(
            [
                Customer(name="NAME " + str(i))
                for i in range(chunk, min(chunk + 10000, n))
            ]
        )
    DBSession.commit()
    print(
        "SQLAlchemy ORM bulk_save_objects(): Total time for " + str(n) +
        " records " + str(time.time() - t0) + " secs")


def test_sqlalchemy_orm_bulk_insert(n=100000):
    init_sqlalchemy()
    t0 = time.time()
    for chunk in range(0, n, 10000):
        DBSession.bulk_insert_mappings(
            Customer,
            [
                dict(name="NAME " + str(i))
                for i in range(chunk, min(chunk + 10000, n))
            ]
        )
    DBSession.commit()
    print(
        "SQLAlchemy ORM bulk_insert_mappings(): Total time for " + str(n) +
        " records " + str(time.time() - t0) + " secs")


def test_sqlalchemy_core(n=100000):
    init_sqlalchemy()
    t0 = time.time()
    engine.execute(
        Customer.__table__.insert(),
        [{"name": 'NAME ' + str(i)} for i in range(n)]
    )
    print(
        "SQLAlchemy Core: Total time for " + str(n) +
        " records " + str(time.time() - t0) + " secs")


def init_sqlite3(dbname):
    conn = sqlite3.connect(dbname)
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS customer")
    c.execute(
        "CREATE TABLE customer (id INTEGER NOT NULL, "
        "name VARCHAR(255), PRIMARY KEY(id))")
    conn.commit()
    return conn


def test_sqlite3(n=100000, dbname='sqlite3.db'):
    conn = init_sqlite3(dbname)
    c = conn.cursor()
    t0 = time.time()
    for i in range(n):
        row = ('NAME ' + str(i),)
        c.execute("INSERT INTO customer (name) VALUES (?)", row)
    conn.commit()
    print(
        "sqlite3: Total time for " + str(n) +
        " records " + str(time.time() - t0) + " sec")

if __name__ == '__main__':
    test_sqlalchemy_cache_bulk()
    #test_sqlalchemy_cache()
    test_sqlalchemy_orm(100000)
    test_sqlalchemy_orm_pk_given(100000)
    #test_sqlalchemy_orm_bulk_save_objects(100000)
    #test_sqlalchemy_orm_bulk_insert(100000)
    test_sqlalchemy_core(100000)
    test_sqlite3(100000)