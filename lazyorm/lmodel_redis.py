def meta_append_redis_methods(name, attrs, is_async):
    assert isinstance(attrs, dict)
    assert isinstance(is_async, bool)
