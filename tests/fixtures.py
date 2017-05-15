import pytest
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


Base = declarative_base()


@pytest.fixture(params=('sqlite', 'postgresql'))
def session(request):
    engine = globals()[request.param]()

    def fin():
        sess.close()
        Base.metadata.drop_all(engine)

    Base.metadata.create_all(engine)
    request.addfinalizer(fin)
    sess = sessionmaker(bind=engine)()
    return sess


@pytest.fixture()
def mysql():
    return create_engine('mysql://root@127.0.0.1:3306/test')


@pytest.fixture()
def sqlite():
    return create_engine('sqlite:///:memory:')


@pytest.fixture()
def postgresql():
    return create_engine('postgresql://postgres:123@127.0.0.1:5432')
