from .session import Session
from .query import Query
from . import scoped_session
from .. import event
from . import loading
from .interfaces import UserDefinedOption
from enum import Enum
from .. import create_engine
from hashlib import md5

class CachedQuery(Query):
    # Do we need this? Probably not.
    pass

class CachedSessionManager(scoped_session):

    def __init__(self, session_factory, scopedfunc=None):
        super().__init__(session_factory, scopedfunc)
        self.cache = SimpleORMCache()
        self.session_factory.kw['cache'] = self.cache
        #self.cache.listen_on_session(self.session_factory)
        self.configure(cache=self.cache)
    
    # TODO: we need to make this a global session manager that coordinates resources for best cache perfomance

class InMemoryDBCache(object):

    def __init__(self):
        #might also be able to just do sqlite:// instead of sqlite:///:memory:
        self.engine = create_engine('sqlite:///:memory:', echo=True)
        self.conn = engine.connect() #keep a warm connection so we don't have overhead of creating this every time

        metadata = Metadata()
        cache = Table('cache', metadata,
                Column('table_name', String),
                Column('group_by', String),
                Column('where', String),
                Column('column', String),

                Column('value', String) 
            ) #value represents the serialized object
    
    #might need to change params for this based on how we want to pass things in.
    def insert(self, table_name, group_by, where, column, value):
        #ideally table_name, group_by, where, column are all strings. We only need to pickle value.
        pass
        
        
"""
Code below inspired by the exmample dogpile caching walkthrough
"""

# not needed?
class CacheValue(Enum):
    NO_VALUE = object()


class ORMCache(object):

    """An add-on for an ORM :class:`.Session` optionally loads full results
    from a memory-loaded cache.
    """

    # TODO: we need to actually make the cache lol

    def __init__(self, cache_key_seed=None):
        self._statement_cache = {}
        self.cache_key_seed = cache_key_seed


    def listen_on_session(self, session_factory):
        event.listen(session_factory, "do_orm_execute", self._do_orm_execute)

    def _generate_cache_key(self, statement, parameters):
        #cache key needs to be a tuple of hashes, or a hash of the hashes.
        #tuple of hashes allows for marginal matching but uses more space.
        #hash of hashes reduces space used but removes marginal matching functionality.
        #see base.py line 700. -> Adding a _generate_opyql_cache_key() func.
        #and traversals.py 
        #https://pypi.org/project/sqlparse/ -> parser
        
        statement_cache_key = statement._generate_cache_key()

        key = statement_cache_key.to_offline_string(
            self._statement_cache, statement, parameters
        ) + repr(self.cache_key_seed)
        return md5(key.encode("ascii")).hexdigest()

    def _do_orm_execute(self, orm_context):
        # TODO: implement our caching scheme here
        for opt in orm_context.user_defined_options: # we don't need to loop here since this function is only called if we want the cache
            if isinstance(opt, RelationshipCache):
                opt = opt._process_orm_context(orm_context)
                if opt is None:
                    continue

            if isinstance(opt, FromCache): # we don't need this since we can assume if we got here the user wants caching
                dogpile_region = self.cache_regions[opt.region] # instead, dynamically allocate the region in the cached session manager above

                our_cache_key = self._generate_cache_key(
                    orm_context.statement, orm_context.parameters
                )

                if opt.ignore_expiration:
                    cached_value = dogpile_region.get( # replace this with our caching scheme
                        our_cache_key,
                        expiration_time=opt.expiration_time,
                        ignore_expiration=opt.ignore_expiration,
                    )
                else:

                    def createfunc():
                        return orm_context.invoke_statement().freeze()

                    cached_value = dogpile_region.get_or_create( # as above, replace this
                        our_cache_key,
                        createfunc,
                        expiration_time=opt.expiration_time,
                    )

                if cached_value is CacheValue.NO_VALUE:
                    # keyerror?   this is bigger than a keyerror...
                    raise KeyError()

                orm_result = loading.merge_frozen_result(
                    orm_context.session,
                    orm_context.statement,
                    cached_value,
                    load=False,
                )
                return orm_result()

        else:
            return None

    def invalidate(self, statement, parameters, opt):
        # NOTE: we can still make use of this of course
        """Invalidate the cache value represented by a statement."""

        statement = statement.__clause_element__()

        dogpile_region = self.cache_regions[opt.region]

        cache_key = opt._generate_cache_key(statement, parameters, self)

        dogpile_region.delete(cache_key)

    def get_or_create(self, cache_key, createfunc, expiration_time):
        # TODO: fill this out, but make it threaded (i.e. have the creater thread(s?) call the createfunc function)
        pass

class SimpleORMCache(ORMCache):
    
    def __init__(self, cache_key_seed=None):
        """
        Creates a simple implementation of an ORMCache
        """
        super().__init__(cache_key_seed=cache_key_seed)
        self.cache = {}
    
    def _do_orm_execute(self, orm_context):
        our_cache_key = self._generate_cache_key(
            orm_context.statement, orm_context.parameters
        )
        def createfunc():
            return orm_context.invoke_statement().freeze()

        if our_cache_key not in self.cache:
            result = self.cache[our_cache_key] = createfunc()
        else:
            result = self.cache[our_cache_key]
        orm_result = loading.merge_frozen_result(orm_context.session, orm_context.statement, result, load=False)
        return orm_result()

    def get(self, key, table_key=None):
        return self.cache.get(key)

    def put(self, key, instance, table_key=None):
        self.cache[key] = instance
        


# Next two classes will not be needed in our scheme, left them here for inspiration

class FromCache(UserDefinedOption):
    """Specifies that a Query should load results from a cache."""

    propagate_to_loaders = False

    def __init__(
        self,
        region="default",
        cache_key=None,
        expiration_time=None,
        ignore_expiration=False,
    ):
        """Construct a new FromCache.

        :param region: the cache region.  Should be a
         region configured in the dictionary of dogpile
         regions.

        :param cache_key: optional.  A string cache key
         that will serve as the key to the query.   Use this
         if your query has a huge amount of parameters (such
         as when using in_()) which correspond more simply to
         some other identifier.

        """
        self.region = region
        self.cache_key = cache_key
        self.expiration_time = expiration_time
        self.ignore_expiration = ignore_expiration

    def _gen_cache_key(self, anon_map, bindparams):
        return None

    def _generate_cache_key(self, statement, parameters, orm_cache):
        statement_cache_key = statement._generate_cache_key()

        key = statement_cache_key.to_offline_string(
            orm_cache._statement_cache, statement, parameters
        ) + repr(self.cache_key)

        return md5(key.encode("ascii")).hexdigest()


class RelationshipCache(FromCache):
    """Specifies that a Query as called within a "lazy load"
    should load results from a cache."""

    propagate_to_loaders = True

    def __init__(
        self,
        attribute,
        region="default",
        cache_key=None,
        expiration_time=None,
        ignore_expiration=False,
    ):
        """Construct a new RelationshipCache.

        :param attribute: A Class.attribute which
         indicates a particular class relationship() whose
         lazy loader should be pulled from the cache.

        :param region: name of the cache region.

        :param cache_key: optional.  A string cache key
         that will serve as the key to the query, bypassing
         the usual means of forming a key from the Query itself.

        """
        self.region = region
        self.cache_key = cache_key
        self.expiration_time = expiration_time
        self.ignore_expiration = ignore_expiration
        self._relationship_options = {
            (attribute.property.parent.class_, attribute.property.key): self
        }

    def _process_orm_context(self, orm_context):
        current_path = orm_context.loader_strategy_path

        if current_path:
            mapper, prop = current_path[-2:]
            key = prop.key

            for cls in mapper.class_.__mro__:
                if (cls, key) in self._relationship_options:
                    relationship_option = self._relationship_options[
                        (cls, key)
                    ]
                    return relationship_option

    def and_(self, option):
        """Chain another RelationshipCache option to this one.

        While many RelationshipCache objects can be specified on a single
        Query separately, chaining them together allows for a more efficient
        lookup during load.

        """
        self._relationship_options.update(option._relationship_options)
        return self