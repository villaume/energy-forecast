from __future__ import annotations

from dotenv import load_dotenv


def pytest_sessionstart(session) -> None:
    load_dotenv()
