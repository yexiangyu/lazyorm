class LazyNode(object):

    def get(self, **kw):
        raise AttributeError()

    def put(self, **kw):
        raise AttributeError()

    def search(self, **kw):
        raise AttributeError()

    def delete(self, **kw):
        raise AttributeError()


class AsyncLazyNode(object):
    async def get(self, **kw):
        raise AttributeError()

    async def put(self, **kw):
        raise AttributeError()

    async def search(self, **kw):
        raise AttributeError()

    async def delete(self, **kw):
        raise AttributeError()
