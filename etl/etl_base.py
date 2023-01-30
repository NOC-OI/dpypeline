from abc import ABC, abstractmethod


class ETLBase(ABC):
    @abstractmethod
    def _extract(self):
        pass

    @abstractmethod
    def _transform(self):
        pass

    @abstractmethod
    def _load(self):
        pass

    def run(self, *args, **kwargs):
        self._extract(*args, **kwargs)
        self._transform(*args, **kwargs)
        self._load(*args, **kwargs)
