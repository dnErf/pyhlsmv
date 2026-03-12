from typing import Any, Optional, Iterator

class SkipListNode:
    def __init__(self, key, value, level:int):
        self.key = key
        self.value = value
        self.hash = self._calculate_hash()
        self.forward:list[Optional["SkipListNode"]] = [None] * (level + 1)

    def _calculate_hash(self) -> str:
        import hashlib
        data = f"{self:key}:{self.value}".encode("utf-8")
        return hashlib.sha256(data).hexdigest()

class SkipList:
    def __init__(self, max_level:int = 16, p:float = 0.5):
        self.max_level = max_level
        self.p = p
        self.header = SkipListNode(None, None, max_level)
        self.level = 0

    def random_level(self):
        import random
        level = 0
        while random.random() < self.p and level < self.max_level:
            level += 1
        return level
    
    def insert(self, key, value) -> None:
        update = [None] * (self.max_level + 1)
        current = self.header

        for i in range(self.level, -1, -1):
            while current.forward[i] and current.forward[i].key < key:
                current = current.forward[i]
            update[i] = current

        current = current.forward[0]

        if current and current.key == key:
            current.value = value
            current.hash = current._calculate_hash()
        else:
            new_level = self.random_level()
            if new_level > self.level:
                for i in range(self.level + 1, new_level + 1):
                    update[i] = self.header
                self.level = new_level
            
            new_node = SkipListNode(key, value, new_level)
            for i in range(new_level + 1):
                new_node.forward[i] = update[i].forward[i]
                update[i].forward[i] = new_node

    def search(self, key) -> Optional[Any]:
        current = self.header
        for i in range(self.level, -1, -1):
            while current.forward[i] and current.forward[i].key < key:
                current = current.forward[i]
        current = current.forward[0]
        if current and current.key == key:
            return current.value
        return None
    
    def delete(self, key) -> bool:
        update = [None] * (self.max_level + 1)
        current = self.header

        for i in range(self.level, -1, -1):
            while current.forward[i] and current.forward[i].key < key:
                current = current.forward[i]
            update[i] = current

        current = current.forward[0]
        if current and current.key == key:
            for i in range(self.level + 1):
                if update[i].forward[i] != current:
                    break
                update[i].forward[i] = current.forward[i]
            
            while self.level > 0 and self.header.forward[self.level] is None:
                self.level -= 1

            return True
        return False

    def get_root(self):
        import hashlib

        hashes = []
        current = self.header.forward[0]
        while current:
            if current.key is not None:
                hashes.append(current.hash)
            current = current.forward[0]
        
        if not hashes:
            return ""
        while len(hashes) > 1:
            new_hashes = []
            for i in range(0, len(hashes), 2):
                left = hashes[i]
                right = hashes[i + 1] if i + 1 < len(hashes) else left
                combined = left + right
                new_hashes.append(hashlib.sha256(combined.encode("utf-8")).hexdigest())
            hashes = new_hashes

        return hashes[0] if hashes else ""
    
    def __iter__(self) -> Iterator[tuple[Any, Any]]:
        current = self.header.forward[0]
        while current:
            yield current.key, current.value
            current = current.forward[0]

    def __len__(self) -> int:
        count = 0
        current = self.header.forward[0]
        while current:
            count += 1
            current = current.forward[0]
        return count
