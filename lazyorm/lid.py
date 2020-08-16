from nanoid import generate

ALPHABET = '1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'


def gen_id(size=24):
    return generate(ALPHABET, size)
