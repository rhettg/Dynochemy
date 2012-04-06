dynochemy: Clever pythonic and async interface to Amazon DynamoDB
=========================

Yet another python inteface for Amazon Dynamo. The API is somewhat inspired by SQLAlchemy.


Features
--------

- Synchronous and Async Support (using Tornado)
- Full API abstraction (rather than needing to know internals of how Dynamo actually needs it's JSON formatted.

Status
------
Under heavy development, just barely functioning. You can get and put things to the db in both sync and async modes.

Really just useful for seeing how the API would work if this library was completed

Example Use
---

Connect to your database:

    db = dynochemy.DB('RhettDB', ('user', 'time'), ACCESS_KEY, ACCESS_SECRET)

This is using a table with both Hash and Range keys (user and time).

    create_time = time.time()

    db[('123', create_time)] = {'full_name': 'Rhett Garber'}

    print db[('123', create_time)

This does a synchronous operations on a dictionary like database.
But wait, there is more:

    d1 = db.put_defer({'user': '123', 'time': create_time, 'full_name': 'Rhett Garber'})
    d2 = db.put_defer({'user': '124', 'time': create_time, 'full_name': 'Rhettly Garber'})
    d3 = db.put_defer({'user': '125', 'time': create_time, 'full_name': 'Rhettford Garber'})
    defer.wait_all([d1, d2, d3])

This does 3 puts simulatenously.
If within an existing tornado environment, you can do something like:

    db = dynochemy.DB('RhettDB', ('user', 'time'), ACCESS_KEY, ACCESS_SECRET, ioloop=IOLoop.instance())

    user = yield tornado.gen.Task(db.get_async, ('123', create_time))

(If you're not familiar with the brilliant tornado.gen stuff, you should be.


Querying has a nice API for it as well:

    result = db.query('123').reverse().limit(2)()
    for item in result:
        print item['full_name']

And of course you do this async as well:

    result = yield tornado.gen.Task(db.query('123').range(t0, t1))

