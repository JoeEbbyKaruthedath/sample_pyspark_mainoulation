import pytest
from lib.Utils import get_spark_session
from lib.DataReader import read_customers, read_orders
from lib.DataManipulation import filter_closed_orders, filter_order_generic
from lib.ConfigReader import get_app_config

#spark is imported from conftest.py automatically as the system will detect it as a fixture(setup) from the filename (conftest.py)

def test_read_customers_df(spark):
    customers_count = read_customers(spark, "LOCAL").count()
    assert customers_count == 12435

#markers are used to mark the testing function by what it does, you can define the transformation in the pytest.ini file
@pytest.mark.transformation()
def test_read_orders_df(spark):
    orders_count = read_orders(spark, "LOCAL").count()
    assert orders_count == 68884

@pytest.mark.transformation()
def test_filter_closed_orders(spark):
    orders_df = read_orders(spark,"LOCAL")
    filtered_count = filter_closed_orders(orders_df).count()
    assert filtered_count == 7556

@pytest.mark.skip("work in progress") #If you want to skip any of the testing functions, skip is a system defind marker
def test_read_app_config():
    config = get_app_config("LOCAL")
    assert config["orders.file.path"] == 'data/orders.csv'

@pytest.mark.skip()
def test_check_closed_count(spark):
    orders_df = read_orders(spark,"LOCAL")
    filtered_count = filter_order_generic(orders_df, "CLOSED").count()
    assert filtered_count == 7556


@pytest.mark.skip()
def test_check_pendingpayment_count(spark):
    orders_df = read_orders(spark,"LOCAL")
    filtered_count = filter_order_generic(orders_df, "PENDING_PAYMENT").count()
    assert filtered_count == 15030


@pytest.mark.skip()
def test_check_complete_count(spark):
    orders_df = read_orders(spark,"LOCAL")
    filtered_count = filter_order_generic(orders_df, "COMPLETE").count()
    assert filtered_count == 22900

#This is a more generalized way of running tests in a more parametrized way
@pytest.mark.parametrize(
        "status, count",
        [("CLOSED",7556), 
         ("PENDING_PAYMENT", 15030),
         ("COMPLETE", 22900)
         ]
)
def test_check_count(spark, status, count):
    orders_df = read_orders(spark,"LOCAL")
    filtered_count = filter_order_generic(orders_df, status).count()
    assert filtered_count == count


