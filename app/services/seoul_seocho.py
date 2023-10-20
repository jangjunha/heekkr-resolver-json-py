from typing import AsyncIterable, Iterable

from aiohttp import ClientSession
from heekkr.book_pb2 import Book
from heekkr.common_pb2 import Date, DateTime
from heekkr.holding_pb2 import (
    AvailableStatus,
    HoldingSummary,
    HoldingStatus,
    OnLoanStatus,
    UnavailableStatus,
)
from heekkr.resolver_pb2 import SearchEntity

from app.core import Library, Service, register_service


__all__ = ("SeoulSeochoService",)


PREFIX = "seoul-seocho"


@register_service(PREFIX)
class SeoulSeochoService(Service):
    def __init__(self) -> None:
        self.base = "https://public.seocholib.or.kr"

    async def get_libraries(self) -> Iterable[Library]:
        async with ClientSession(self.base) as session, session.get(
            "/api/common/libraryInfo"
        ) as response:
            root = await response.json()
        return (
            Library(id="{}:{}".format(PREFIX, lib["manageCode"]), name=lib["libName"])
            for lib in root["contents"]["libList"]
            if lib["manageCode"] != "ALL"
        )

    async def search(
        self, keyword: str, library_ids: Iterable[str]
    ) -> AsyncIterable[SearchEntity]:
        async with ClientSession(self.base) as session, session.post(
            "/api/search",
            json={
                "searchKeyword": keyword,
                "manageCode": [
                    lid.removeprefix("seoul-seocho:") for lid in library_ids
                ],
            },
        ) as response:
            root = await response.json()

        for book in root["contents"]["bookList"]:
            yield SearchEntity(
                book=Book(
                    isbn=book["isbn"],
                    title=book["originalTitle"],
                    author=book["originalAuthor"],
                    publisher=book["originalPublisher"],
                    # publish_date=
                ),
                holding_summaries=[
                    HoldingSummary(
                        library_id=f"{PREFIX}:{book['manageCode']}",
                        location=book["regCodeDesc"],
                        call_number=book["callNo"],
                        status=self._parse_state(book),
                    )
                ],
            )

    def _parse_state(self, book: dict) -> HoldingStatus:
        common = dict(
            is_requested=book["reservationCount"] > 0,
            requests=book["reservationCount"],
            requests_available=book["isActiveResvYn"] == "Y",
        )

        loan_status = book["loanStatus"]
        if loan_status == "대출가능":
            return HoldingStatus(
                available=AvailableStatus(
                    detail=book["workingStatus"],
                ),
                **common,
            )
        elif loan_status.startswith("대출불가"):
            match book["workingStatus"]:
                case "대출중" | "상호대차중":
                    return HoldingStatus(
                        on_loan=OnLoanStatus(
                            detail=book["workingStatus"],
                            due=self._parse_due(book["returnPlanDate"]),
                        ),
                        **common,
                    )
        return HoldingStatus(
            unavailable=UnavailableStatus(
                detail=book["workingStatus"],
            ),
            **common,
        )

    def _parse_due(self, due: str) -> DateTime:
        parts = [int(x) for x in due.split(".")]
        return DateTime(
            date=Date(
                year=parts[0],
                month=parts[1],
                day=parts[2],
            )
        )
