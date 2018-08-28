import hashlib
import uuid
import pyhash
# __hasher = pyhash.fnv1_32()
class HashFunction:
    def __init__(self, length=5, seed=0):
        self.nBins = 2 ** length
        self.seed = seed
        self.mapping = {}
        self._hasher = pyhash.fnv1_32()
    
    def hash(self, value):
        result = self._hasher(value, seed=self.seed) % self.nBins
        if result not in self.mapping:
            self.mapping[result] = []
        self.mapping[result].append(value)
        return result