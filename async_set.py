from typing import Optional, Iterable
from asyncio import Lock

class AsyncSet:
    def __init__(self, values: Optional[Iterable] = None, lock: Optional[Lock] = None):
        self.values = set() if values is None else set(values)
        self.lock = Lock() if lock is None else lock
    
    async def add(self, value: object):
        """Asynchronously adds a value to the set."""
        async with self.lock:
            self.values.add(value)
    
    async def remove(self, value: object):
        """Asynchronously removes a value from the set."""
        async with self.lock:
            self.values.remove(value)
    
    async def contains(self, value: object) -> bool:
        """Asynchronously checks if a value is in the set."""
        async with self.lock:
            return value in self.values
        
    async def size(self) -> int:
        """Asynchronously returns the number of values in the set."""
        async with self.lock:
            return len(self.values)
    
    async def add_if_missing(self, value: object) -> bool:
        """Asynchronously adds a value to the set and returns whether it is new."""
        async with self.lock:
            if value in self.values:
                return False
            else:
                self.values.add(value)
                return True