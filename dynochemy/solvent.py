class Solvent(object):
    """A solvent is a abstraction over Dynochemy database operations where
    operations can be combined together and executed with some intelligence.

    This includes:
        * automatic secondary index maintenance
        * throttling
        * memcache write-through caches and invalidation
    """
    def put(self, table, entity):
        raise NotImplementedError

    def delete(self, table, key):
        raise NotImplementedError

    def update(self, table, key, put=None, add=None, delete=None):
        raise NotImplementedError

    def run(self, db):
        raise NotImplementedError

    def run_async(self, db, callback=None):
        raise NotImplementedError

    def run_defer(self, db):
        raise NotImplementedError
