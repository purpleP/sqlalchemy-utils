import pytest
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


Base = declarative_base()


@pytest.fixture(params=('sqlite', 'mysql'))
def session(request):
    engine = request.getfuncargvalue(request.param)

    def fin():
        sess.close()
        Base.metadata.drop_all(engine)

    Base.metadata.create_all(engine)
    request.addfinalizer(fin)
    sess = sessionmaker(bind=engine)()
    return sess


@pytest.fixture()
def mysql(request):
    return create_engine('mysql://root:root@127.0.0.1:3306/test')


@pytest.fixture()
def sqlite():
    return create_engine('sqlite:///:memory:')
