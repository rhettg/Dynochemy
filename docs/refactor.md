Big Refactor / Redesign
====

Previously, there were 3 ways to use the library: 

  * Direct db calls, (dyndb.EntityTable.get(key))
  * Operation (operation.GetOperation(table, key).run(self.dyndb))
  * Solvent (solvent.get(dyndb.EntityTable, key), solvent.run(self.dyndb))

These are not mutually exclusive, but build on each other. However there is
some complexity there because I didn't really see how it was going to be used
until now. 

We primarily have to use the Solvent method now because you really have to have
the retry/capacity logic of a Solvent.

Also, I was unaware of the existance of 'concurrent.futures', part of python3
that is backported (via pypi) to 2.x. This is effectivly 'defer'. Error
handling needs to be redesigned anyway, and I like futures better. Actually,
it's remarkable how similiar defer is to future, but I guess both were inspired
by Twisted. Where I differed, I think I was wrong.


Modern Usage
----

Globally, we should create a 'Solvent Maker' (similiar to a sqlaqlchemy session maker). When a solvent is generated, it is 
associated with a real Database implementation. In the future, you could associate it with multiple databases and caching layers.

Operations generated during a solvent would then be 'played' against all the backing layers. If the layers are like:

    layers = [cache1, database1, database2]

(with database2 essentially being a replicated database)

Reads are done left to right (so the first read to succeed is done)

Writes are done right to left (writes are done to each layer)

Additonaly, our SolventResult object will allow access of results both by operation key, and by sequence.

Usage can then look:

    try:
        entity = yield tornado.gen.YieldFuture(self.solvent().EntityTable.get(key))
    except dynochemy.ItemNotFoundError, e:
        entity = None

    yield tornado.gen.YieldFuture(self.solvent().EntityTable.put({..})

    query_results = yield tornado.gen.YieldFuture(self.solvent().EntityTable.query(..))
    for result in query_results:
        pass

or

    with self.solvent().begin() as s:
        for key in keys:
            entities_f.append(s.EntityTable.get(key))

    entities = yield [tornado.gen.YieldFuture(entity_f) for entity_f in entities_f]

In sync mode (non-tornado), we still use futures, but you can just do the following:

    entity = self.solvent().EntityTable.get(key).result()

    with self.solvent().begin() as s:
        for key in keys:
            entities_f.append(s.EntityTable.get(key))

    entities = [entity_f.result() for entity_f in entities_f]

Or even the shortcut key index:

    entity = self.solvent().EntityTable[key]

Architecture Overview
----

A solvent is the main coordinator dealing with retries and capacity. It is our
primary interface and should have the nicest api.


A solvent generates lists of operations to 'played' against data stores.

A solvent can also introspect the underlying tables to understand their resource usage.

A solvent operation call returns a 'Future'.

When run, a solvent return a SolventResult object that contains all the results from
executing a solvent. This object can, by key lookups, return the results of any
operation played. If an operation failed, or an exception was generated, the
OperationResult will emit that exception when the result is asked for.
