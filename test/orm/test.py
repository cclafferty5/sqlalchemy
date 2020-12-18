import time
import sqlite3

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Boolean, create_engine
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
    age = Column(Integer())

class User(Base): 
    __tablename__ = "user"   
    classid = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    email = Column(String(255), nullable=False)
    alternate_email = Column(String(255), nullable=True, default=None)
    sid = Column(String(255), nullable=False)
    calnet_uid = Column(String(255), nullable=True, default=None)
    github = Column(String(255), nullable=True, default=None)
    repo = Column(String(255), nullable=True, default=None)
    snaps_repo = Column(String(255), nullable=True, default=None) 
    repo_id = Column(String(255), nullable=True, default=None) 
    repo_hook_key = Column(String(255), nullable=True, default=None) 
    mentor_id = Column(Integer, nullable=True)
    is_mentor_autoassigned = Column(Boolean)
    gradescope_id = Column(Integer, nullable=True)
    gradescope_stage_id = Column(Integer, nullable=True)
    is_mentor = Column(Boolean, nullable=False, default=False)
    is_tutor = Column(Boolean, nullable=True, default=False)
    is_tutor_handler = Column(Boolean, nullable=True, default=False)
    lab_time_offset = Column(Integer, nullable=True)
    automatic_extensions_allowed = Column(Boolean, nullable=True, default=False)
    lab_slip_days = Column(Integer, nullable=True, default=False)
    is_instructor = Column(Boolean, nullable=True, default=False)
    is_self_paced = Column(Boolean, nullable=True, default=False)
    discord_name = Column(String(255), nullable=True, default=None)
    struggling = Column(Boolean, default=False)

def create_user(classid, name, email, alternate_email, sid, calnet_uid, github, repo, snaps_repo, repo_id, repo_hook_key, mentor_id, is_mentor_autoassigned, gradescope_id, gradescope_stage_id, is_mentor, is_tutor, is_tutor_handler, lab_time_offset, automatic_extensions_allowed, lab_slip_days, is_instructor, is_self_paced, discord_name, struggling):
    user = User()
    user.classid = classid
    user.name = name
    user.email = email
    user.alternate_email = alternate_email
    user.sid = sid
    user.calnet_uid = calnet_uid
    user.github = github
    user.repo = repo
    user.snaps_repo = snaps_repo
    user.repo_id = repo_id
    user.repo_hook_key = repo_hook_key
    user.mentor_id = mentor_id
    user.is_mentor_autoassigned = is_mentor_autoassigned
    user.gradescope_id = gradescope_id
    user.gradescope_stage_id = gradescope_stage_id
    user.is_mentor = is_mentor
    user.is_tutor = is_tutor
    user.is_tutor_handler = is_tutor_handler
    user.lab_time_offset = lab_time_offset
    user.automatic_extensions_allowed = automatic_extensions_allowed
    user.lab_slip_days = lab_slip_days
    user.is_instructor = is_instructor
    user.is_self_paced = is_self_paced
    user.discord_name = discord_name
    user.struggling = struggling
    return user

def init_sqlalchemy(dbname='sqlite:///sqlalchemy.db'):
    global engine
    global cache_engine
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

def time_query(queries):
    t0 = time.time()
    for query in queries:
        customers = query.all()
    return time.time() - t0

def test_sqlalchemy_cache_bulk(n=100000):
    init_sqlalchemy()
    for i in range(n):
        customer1 = Customer()
        customer2 = Customer()
        customer1.name = customer2.name = 'NAME 1'
        customer1.age = customer2.age = i % 10
        DBSession.add(customer1)
        CachedDBSession.add(customer2)
        if i % 1000 == 0:
            DBSession.flush()
            CachedDBSession.flush()
    DBSession.commit()
    CachedDBSession.commit()
    t0 = time.time()
    customers = DBSession.query(Customer).filter_by(name='NAME 1', age=5).all()
    no_cache_time = time.time() - t0
    t0 = time.time()
    cached_customers = CachedDBSession.query(Customer).filter_by(name='NAME 1', age=5).all()
    cache_time = time.time() - t0
    print("No cache time:", no_cache_time)
    print("Cached time:", cache_time)

def test_cache_user(n=10000):
    print('TEST CACHE USER N={}'.format(n))

    init_sqlalchemy()
    for i in range(n):
        user1 = create_user(
            i,
            str(i),
            "{}@gmail.com".format(i),
            "{}@gmail.com".format(i+1),
            "i",
            "i",
            "{}@github.com".format(i),
            "{}".format(i),
            "{}".format(i),
            "{}".format(i),
            "{}".format(i),
            i,
            True,
            i%10,
            i,
            i%2==0,
            i%2==1,
            False,
            i,
            True,
            i,
            i%4 == 0,
            i%5 == 0,
            "discord{}".format(i),
            False
        )
        user2 = create_user(
            i,
            str(i),
            "{}@gmail.com".format(i),
            "{}@gmail.com".format(i+1),
            "i",
            "i",
            "{}@github.com".format(i),
            "{}".format(i),
            "{}".format(i),
            "{}".format(i),
            "{}".format(i),
            i,
            True,
            i%10,
            i,
            i%2==0,
            i%2==1,
            False,
            i,
            True,
            i,
            i%4 == 0,
            i%5 == 0,
            "discord{}".format(i),
            False
        )
        DBSession.add(user1)
        CachedDBSession.add(user2)
        if i % 1000 == 0:
            DBSession.flush()
            CachedDBSession.flush()
    DBSession.commit()
    CachedDBSession.commit()

    no_cache_queries = [
        DBSession.query(User).filter(User.gradescope_stage_id < 6),
        DBSession.query(User).filter(User.classid > n/2, User.gradescope_stage_id < 6),
        DBSession.query(User).filter(User.sid > n/2, User.gradescope_stage_id < 6)
    ]
    cache_queries = [
        CachedDBSession.query(User).filter(User.gradescope_stage_id < 6),
        CachedDBSession.query(User).filter(User.classid > n/2, User.gradescope_stage_id < 6),
        CachedDBSession.query(User).filter(User.sid > n/2, User.gradescope_stage_id < 6)
    ]
    time1 = time_query(no_cache_queries)
    time2 = time_query(cache_queries)
    print(time1, time2)
    return time1, time2

def test_cache_customers(n=100000):
    print('TEST CACHE CUSTOMERS N={}'.format(n))
    init_sqlalchemy()
    for i in range(n):
        customer1 = Customer()
        customer2 = Customer()
        customer1.name = customer2.name = 'NAME 1'
        customer1.age = customer2.age = i % 10
        DBSession.add(customer1)
        CachedDBSession.add(customer2)
        if i % 1000 == 0:
            DBSession.flush()
            CachedDBSession.flush()
    DBSession.commit()
    CachedDBSession.commit()

    no_cache_queries = [
        # DBSession.query(Customer),
        DBSession.query(Customer).filter_by(name='NAME 1', age=5),
        DBSession.query(Customer).filter(Customer.age > 3),
    ]
    cache_queries = [
        # CachedDBSession.query(Customer),
        CachedDBSession.query(Customer).filter_by(name='NAME 1', age=5),
        CachedDBSession.query(Customer).filter(Customer.age > 3),
    ]
    time1 = time_query(no_cache_queries)
    time2 = time_query(cache_queries)
    print(time1, time2)
    return time1, time2

def test_cache_pk(n=100000):
    init_sqlalchemy()
    for i in range(n):
        customer1 = Customer()
        customer2 = Customer()
        customer1.name = customer2.name = 'NAME 1'
        customer1.age = customer2.age = i % 10
        DBSession.add(customer1)
        CachedDBSession.add(customer2)
        if i % 1000 == 0:
            DBSession.flush()
            CachedDBSession.flush()
    DBSession.commit()
    CachedDBSession.commit()
    time1, customers1 = time_query(DBSession.query(Customer))
    time2, customers2 = time_query(CachedDBSession.query(Customer))
    
    return time1, time2

def test_cache_iter(f, ns=[1000, 10000, 100000]):
    result = []
    for n in ns:
        no_cache_time, cache_time = f(n)
        print("No cache time at n=%d:" % n, no_cache_time)
        print("Cached time at n=%d:" % n, cache_time)
        hit_rate = CachedDBSession.cache.get_hit_rate()
        print("Cache hit rate:", hit_rate)
        print("------------")
        result.append((no_cache_time, cache_time, hit_rate))
    print(result)



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
    test_cache_iter(test_cache_customers)
    test_cache_iter(test_cache_user)
    #test_sqlalchemy_cache_bulk()
    #test_sqlalchemy_cache()
    # test_sqlalchemy_orm(100000)
    # test_sqlalchemy_orm_pk_given(100000)
    #test_sqlalchemy_orm_bulk_save_objects(100000)
    #test_sqlalchemy_orm_bulk_insert(100000)
    # test_sqlalchemy_core(100000)
    # test_sqlite3(100000)