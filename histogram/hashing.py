import random
import hashlib


class HashFunction:

    def __init__(self, length=5, seed=0):
        self.nBins = 2 ** length
        self.seed = str(seed)
        self.mapping = {}
    

    def hash(self, value):
        result = int(hashlib.md5((self.seed + str(value)).encode()).hexdigest(), 16) % self.nBins
        if result not in self.mapping:
            self.mapping[result] = []
        self.mapping[result].append(value)
        return result


