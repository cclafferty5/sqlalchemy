from .session import Session

class CachedSession(Session):
    pass
    # TODO: we need to make a global resource manager that can get objects out of the identity map not only based on the primary key