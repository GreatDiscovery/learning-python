from typing import List
from typing import Optional
from unittest import TestCase

from sqlalchemy import ForeignKey, select
from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase, Session
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine

engine = create_engine("mysql+mysqldb://root:123456@127.0.0.1:3306/test", echo=True)


class TestMySQLAlchemy(TestCase):

    def test_select(self):
        from sqlalchemy import select
        session = Session(engine)
        stmt = select(MyUser).where(MyUser.name.in_(["gavin", "jiayun"]))
        for user in session.scalars(stmt):
            print(user)

    def test_update(self):
        session = Session(engine)
        stmt = select(MyUser).where(MyUser.name == "gavin")
        user = session.scalars(stmt).one()
        print(user)
        user.age = 15
        session.add(user)
        session.commit()

class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "user_account"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(30))
    fullname: Mapped[Optional[str]]
    addresses: Mapped[List["Address"]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"User(id={self.id!r}, name={self.name!r}, fullname={self.fullname!r})"


class Address(Base):
    __tablename__ = "address"
    id: Mapped[int] = mapped_column(primary_key=True)
    email_address: Mapped[str]
    user_id: Mapped[int] = mapped_column(ForeignKey("user_account.id"))
    user: Mapped["User"] = relationship(back_populates="addresses")

    def __repr__(self) -> str:
        return f"Address(id={self.id!r}, email_address={self.email_address!r})"


class MyUser(Base):
    __tablename__ = "user"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(30))
    age: Mapped[int]

    def __repr__(self) -> str:
        return f"User(id={self.id!r}, name={self.name!r}, age={self.age!r})"
