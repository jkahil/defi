from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
import time

TIMESTAMP_PER_YEAR=86400*360

def todayTimestamp():
    ts = int(time.time())
    return (ts)

def client(api_url):
    """
    Initializes a connection to subgraph
    :param api_url: subgraph url for connection
    :return:
    """
    sample_transport = RequestsHTTPTransport(
        url=api_url,
        verify=True,
        retries=10,
    )
    client = Client(
        transport=sample_transport
    )
    return client

