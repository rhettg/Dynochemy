# -*- coding: utf-8 -*-
"""
This module contains the primary objects that power Bootstrap.

:copyright: (c) 2012 by Firstname Lastname.
:license: ISC, see LICENSE for more details.
"""
import functools
import time

from .errors import Error

class TimeoutError(Error): pass

class Defer(object):
    def __init__(self, ioloop=None, timeout=None):
        self.done = False
        self.ioloop = ioloop
        self.do_stop = False
        self.timeout = timeout
        self._timeout_req = None
        self._callbacks = []

    def callback(self, *args, **kwargs):
        self.done = True
        self.args = args
        self.kwargs = kwargs

        if self._timeout_req:
            self.ioloop.remove_timeout(self._timeout_req)
            self._timeout_req = None

        if self.do_stop:
            self.ioloop.stop()

        for c in self._callbacks:
            c(self)

    def __call__(self):
        if not self.ioloop:
            raise ValueError("IOLoop required")

        if not self.done:
            self.do_stop = True
            if self.timeout:
                self._timeout_req = self.ioloop.add_timeout(time.time() + self.timeout, functools.partial(self.callback, error='Timeout'))

            self.ioloop.start()

        assert self.done
        return (self.args, self.kwargs)

def wait_all(all_deferred, timeout=None):
    ioloop = all_deferred[0].ioloop

    timeout_req = None
    if timeout:
        def timeout():
            ioloop.stop()
            raise TimeoutError()

        timeout_req = ioloop.add_timeout(time.time() + timeout, timeout)

    def callback(d):
        assert d.done
        all_deferred.remove(d)

        if len(all_deferred) == 0:
            if timeout_req:
                ioloop.remove_timeout(timeout_req)
            ioloop.stop()

    for d in all_deferred:
        d.add_callback(callback)

    ioloop.start()
