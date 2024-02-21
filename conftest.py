import pytest
from lib.Utils import get_spark_session

@pytest.fixture #fixture is to write the setup code
def spark():
    '''
    Creates the spark session for the tests
    '''
    spark_session = get_spark_session("LOCAL")
    #Code upto yeild will run before the unit test and the code after yeild will run after the unit test for returning the resources, also known as teardown
    yield spark_session #instead of return, yeild is used as this will help to close and end the resources once the job is done
    spark_session.stop()
