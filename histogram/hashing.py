import hashlib


class HashFunction:

    def __init__(self, length=5, seed=0):
        self.nBins = 2 ** length
        self.seed = str(seed)
        self.mapping = {}
    

    def hash(self, value, store=False):
        # hash_func = hashlib.md5
        hash_func = hashlib.sha224
        result = int(hash_func((self.seed + str(value)).encode()).hexdigest(), 16) % self.nBins
        if store:
            if result not in self.mapping:
                self.mapping[result] = []
            self.mapping[result].append(value)
        return result

    def reset_mapping(self):
        self.mapping = {}


