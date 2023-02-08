from abc import ABC, abstractmethod


class ETLBase(ABC):
    @abstractmethod
    def _extract(self, *args, **kwargs):
        pass

    @abstractmethod
    def _transform(self, *args, **kwargs):
        pass

    @abstractmethod
    def _load(self, *args, **kwargs):
        pass

    def run(self, *args, **kwargs):
        self._extract(*args, **kwargs)
        self._transform(*args, **kwargs)
        self._load(*args, **kwargs)
