import nanoid


def lazyid():
    return nanoid.generate("1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ", 48)
