# -*- coding: utf-8 -*-
"""
This module contains tools for doing defered async operations.
Some of this is inspired by Twisted, but they are pretty different.

:copyright: (c) 2012 by Rhett Garber.
:license: ISC, see LICENSE for more details.
"""
import functools
import time
import sys

from .errors import Error

class TimeoutError(Error): pass

class Defer(object):
    def __init__(self, ioloop=None):
        self.done = False
        self.ioloop = ioloop
        self.do_stop = False
        self.args = None
        self.kwargs = None
        self._timeout_req = None
        self._callbacks = []
        self._exception = None

    def add_callback(self, cb):
        self._callbacks.append(cb)
        if self.done:
            cb(self)

    def _do_done(self):
        if self.done:
            raise ValueError("Already called")

        self.done = True

        if self._timeout_req:
            self.ioloop.remove_timeout(self._timeout_req)
            self._timeout_req = None

        if self.do_stop:
            self.ioloop.stop()

        for c in self._callbacks:
            c(self)

    def callback(self, *args, **kwargs):
        """Handle result (defer is now complete"""
        self.args = args
        self.kwargs = kwargs

        self._do_done()

    def exception(self, ex_type=None, ex=None, tb=None):
        """Handle error result (defer is now complete
        
        Caller can pass in exception info, or if inside a try/except block, we
        can pull it from sys.exc_info()
        """
        if ex is None:
            ex_type, ex, tb = sys.exc_info()

        if ex and not ex_type:
            ex_type = type(ex)

        self._exception = (ex_type, ex, tb)

        self._do_done()

    def __call__(self, timeout=None):
        if not self.done and not self.ioloop:
            raise ValueError("IOLoop required")

        if not self.done:
            self.do_stop = True
            if timeout is not None:
                self._timeout_req = self.ioloop.add_timeout(time.time() + timeout, functools.partial(self.callback, error='Timeout'))

            self.ioloop.start()

        assert self.done
        return self.result

    def rethrow(self):
        if self._exception:
            ex_type, ex, tb = self._exception
            raise ex_type, ex, tb

    @property
    def error(self):
        if self._exception:
            return self._exception[1]

        return None

    @property
    def result(self):
        self.rethrow()

        if len(self.args) == 1 and not self.kwargs:
            return self.args[0]
        else:
            return self.args, self.kwargs


class ResultErrorTupleDefer(Defer):
    """Special Defer who's callback is in the form of (result, error)
    """
    @property
    def result(self):
        if len(self.args) == 2:
            return self.args[0], self.args[1]
        elif len(self.args) == 1:
            return self.args[0], None
        else:
            raise ValueError(self.args)


class ResultErrorKWDefer(Defer):
    """Special defer who's callback is in the form (result, error=ERROR)

    It will return a tuple of (result, error)
    """
    @property
    def result(self):
        if self.args:
            return self.args[0], self.kwargs.get('error')
        else:
            return None, self.kwargs.get('error')


def wait_all(all_deferred, timeout=None):

    if len(all_deferred) == 0:
        return

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
            if ioloop: 
                ioloop.stop()

    done_defer = [d for d in all_deferred if d.done]
    [callback(d) for d in done_defer]

    for d in all_deferred:
        d.add_callback(callback)

    if all_deferred:
        ioloop.start()
