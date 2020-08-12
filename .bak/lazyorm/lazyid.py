import nanoid

ALPHABET = '1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'


def lazyid(size=32):
    """generate nano id

    Args:
        size (int, optional): [description]. Defaults to 32.

    Returns:
        str: len(str) = size, according to https://alex7kom.github.io/nano-nanoid-cc, no collision will happened with even dozen of millsions of id generate per second.
    """
    return nanoid.generate(ALPHABET, size)
