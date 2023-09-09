from collections import deque


class FixedQueue(deque):
    def __init__(self, size):
        super().__init__(maxlen=size)

    def append(self, item):
        super().append(item)
