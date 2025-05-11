from abc import ABC, abstractmethod


class BaseReader(ABC):
    @abstractmethod
    def read(self):
        raise NotImplementedError


class BaseReaderBuilder(ABC):
    @abstractmethod
    def build(self) -> BaseReader:
        raise NotImplementedError
