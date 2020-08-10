class LazyNode(object):

    def get(self, **kw):
        raise AttributeError()

    def put(self, **kw):
        raise AttributeError()

    def search(self, **kw):
        raise AttributeError()

    async def aget(self, **kw):
        raise AttributeError()

    async def aput(self, **kw):
        raise AttributeError()

    async def asearch(self, **kw):
        raise AttributeError()
