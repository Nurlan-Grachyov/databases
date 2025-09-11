from sqlalchemy import Column, Integer, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Genre(Base):
    __tablename__ = 'genres'
    # id, title
    pass


class Book(Base):
    __tablename__ = 'books'

    # id, title, price, quantity, genre's id, author's id
    genre_id = Column(Integer, ForeignKey('genres.id'))
    genre = relationship('Genre', back_populates='genres')

    author_id = Column(Integer, ForeignKey(''))
    author = relationship('Author', back_popelates='authors')

    order = relationship('Order', back_populates='orders')


class Author(Base):
    __tablename__ = 'author'

    # id, name


class City(Base):
    __tablename__ = 'cities'

    # id, city's name


class Client(Base):
    __tablename__ = ' clients'

    # id, name, mail, city
    city_id = Column(Integer, ForeignKey('cities'))
    city = relationship('City', back_populates='cities')


class Order(Base):
    __tablename__ = 'orders'

    # id, client's wishes
    book = relationship('Book', back_populates='books')


class OrderBook(Base):
    __tablename__ = 'order_book_assoc'
    order_id = Column(Integer, ForeignKey('orders.id'), primary_key=True)
    book_id = Column(Integer, ForeignKey('books.id'), primary_key=True)
    quantity = Column(Integer, nullable=False)

    order = relationship('Order', back_populates='order_books')
    book = relationship('Book', back_populates='order_books')


class Step(Base):
    __tablename__ = 'steps'
    # id, step's tittle
    pass


class StepOrder(Base):
    __tablename__ = 'step_order_assoc'
    step_id = Column(Integer, ForeignKey('steps.id', primary_key=True))
    order_id = Column(Integer, ForeignKey('orders.id', primary_key=True))
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    step = relationship('Step', back_populates='step_orders')
    order = relationship('Order', back_populates='step_orders')
