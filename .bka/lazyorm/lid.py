from nanoid import generate

ALPHABET = '1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_'


def lid(size=32):
    return generate(ALPHABET, size)
