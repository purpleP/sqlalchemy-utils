import pytest
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


Base = declarative_base()


@pytest.fixture(params=('sqlite', 'postgresql'))
def session(request):
    engine = globals()[request.param]()
    Base.metadata.create_all(engine)
    sess = sessionmaker(bind=engine)()
    yield sess
    sess.close()
    Base.metadata.drop_all(engine)


@pytest.fixture()
def mysql():
    return create_engine('mysql://root@127.0.0.1:3306/test')


@pytest.fixture()
def sqlite():
    return create_engine('sqlite://')


@pytest.fixture()
def postgresql():
    return create_engine('postgresql://postgres:123@127.0.0.1:5432')