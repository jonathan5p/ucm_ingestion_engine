from abc import ABC, abstractmethod


class BaseWriter(ABC):
    @abstractmethod
    def write(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def add_metadata(self, *args, **kwargs):
        raise NotImplementedError


class BaseWriterBuilder(ABC):
    @abstractmethod
    def build(self) -> BaseWriter:
        raise NotImplementedError
