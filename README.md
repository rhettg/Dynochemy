dynochemy: Clever pythonic and async interface to Amazon DynamoDB
=========================

Yet another python inteface for Amazon Dynamo. The API is somewhat inspired by SQLAlchemy.


Features
--------

- Synchronous and Async Support (using Tornado)
- Full API abstraction (rather than needing to know internals of how Dynamo actually needs it's JSON formatted.
- High-level constructs for dealing with provisioning throughput limits and maintaining secondary indexes.
- SQL-backend for testing 

Status
------
Under heavy development, just barely functioning. I would stay away except for
experimental use or you really want to get involved with development yourself.

Really just useful for seeing how the API would work if this library was completed

### Known TODOs

  * Solvent: Needs scan support
  * Views: Need the ability to regenerate views from scratch, or even partial rewrites to help with failure scenarios.
  * Solvent: Need throttling support rather than always hammer until provisioning error.

Example Use
---

Describe your tables:

    class MyTable(dynochemy.Table):
        name = 'my_table'
        hash_key = 'user'
        range_key = 'time'

        read_capacity = 1000
        write_capacity = 50

Connect to your database:

    db = dynochemy.DB(ACCESS_KEY, ACCESS_SECRET)
    db.register(MyTable)

This is using a table with both Hash and Range keys (user and time).

    create_time = time.time()

    db.MyTable.put({'user': '123', 'time': create_time, 'full_name': 'Rhett Garber'})

    print db.MyTable.get(('123', create_time))

This does a synchronous operations on a dictionary like database.
But wait, there is more:

    d1 = db.MyTable.put_defer({'user': '123', 'time': create_time, 'full_name': 'Rhett Garber'})
    d2 = db.MyTable.put_defer({'user': '124', 'time': create_time, 'full_name': 'Rhettly Garber'})
    d3 = db.MyTable.put_defer({'user': '125', 'time': create_time, 'full_name': 'Rhettford Garber'})
    defer.wait_all([d1, d2, d3])

This does 3 puts simulatenously.
If within an existing tornado environment, you can do something like:

    db = dynochemy.DB(ACCESS_KEY, ACCESS_SECRET, ioloop=IOLoop.instance())

    user = yield tornado.gen.Task(db.MyTable.get_async, ('123', create_time))

(If you're not familiar with the brilliant tornado.gen stuff, you should be.


Querying has a nice API for it as well:

    result = db.MyTable.query('123').reverse().limit(2)()
    for item in result:
        print item['full_name']

And of course you do this async as well:

    result = yield tornado.gen.Task(db.query('123').range(t0, t1).async())

One of the great features of DynamoDB is how it can do atomic counters using
the 'update' command. Dynochemy can support that too:

    db.MyTable.update((hash_key, range_key), add={'counter_1': 1, 'counter_2': 1}, put={'time_modified': time.time()})

This would update the indicated element (or create it if it doesn't exist),
increment (or create) the counters and then set the time modified field.


High-level Dynochemy: Solvent
----

The above sample API is your simple, direct access to the database. However, in
production use, I've found that dealing with errors, multiple requests and the
like really cries for a high-level abstraction.

So, we have what are called 'Solvents'.

Solvents are an abstraction which generates a list of operations to be
completed. It figures out how to combine all those operations into individual
requests, runs them against a database and deals with throttling for you.

Example:

    s = dynochemy.Solvent()

    s.put(MyTable, entity_1)
    s.put(MyTable, entity_2)
    s.put(MyTable, entity_3)

    get_op_1 = s.get(MyTable, '123')
    get_op_2 = s.get(MyTable, '124')

    result = s.run(db)

    entity = result[get_op_1]
    print entity

This example will run simulataneously, a BatchWrite and a BatchGetItem request. If
part of the batch fails because of too many writes, it will be transparently
retried a few times, after a delay. Each call to add an operation returns an
operation object that can be used as a key to get the resulting values and
errors.

### Views

In addition to intelligently combining operations together for better performance, a solvent can maintain 'views'.

A view is another table full of calculated, or processed data, based on data in the original table. So for example, if you
wanted to create counts of how many entities in your table had a certain value, you could create a view.

    class CountTable(Table):
        table_name = 'count_table'
        hash_key = 'value'

    class EntityValueCountView(View):
        table = EntityTable
        view_table = CountTable

        @classmethod
        def add(cls, entity):
            return [UpdateOperation(cls.view_table, entity['value'], add={'count': 1})]

        @classmethod
        def remove(cls, entity):
            return [UpdateOperation(cls.view_table, entity['value'], add={'count': -1})]

    db.register(CountTable)
    db.register(EntityValueCountView)

After registering the view, any writes to the 'EntityTable' using a solvent
will also be run through your view implementation, allowing the secondary table
be kept in perfect sync.

SQL-Backed Dynochemy
----
Primarily useful for testing, you can point Dynochemy at a SQLALchemy compatable database and use the same API.

    engine = sqlalchemy.create_engine('sqlite:///')
    db = dynochemy.SQLDB(engine)

This SQL backend is pretty fully functional, right down to simulating provisioning throughput calculations.

Keep in mind, that async operations are behind the scenes syncronous, as there are no good SQL apis for that.
