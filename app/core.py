import abc
import dataclasses
from typing import AsyncIterable, Callable, Iterable

from heekkr.resolver_pb2 import SearchEntity


@dataclasses.dataclass
class Coordinate:
    latitude: float
    longitude: float


@dataclasses.dataclass
class Library:
    id: str
    name: str
    coordinate: Coordinate | None = None


class Service(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def get_libraries(self) -> Iterable[Library]:
        ...

    @abc.abstractmethod
    async def search(
        self, keyword: str, library_ids: Iterable[str]
    ) -> AsyncIterable[SearchEntity]:
        ...


def register_service(name: str) -> Callable[[type[Service]], type[Service]]:
    def inner(service: type[Service]) -> type[Service]:
        services[name] = service()
        return service

    return inner


services: dict[str, Service] = {}
from app.services import *  # noqa: E402, F403
