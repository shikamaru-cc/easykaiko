# easykaiko - kaiko api wrapper
#
# Examples:
#
# client = easykaiko.Client('<your-api-key>');
#
# # get ohlcv data
# params = {
#     'start_time': '2024-01-01T00:00:00Z',
#     'end_time': '2025-01-01T00:00:00Z',
#     'interval': '1d',
#     'sort': 'desc'
# }
# data = client.get_ohlcv('cbse', 'spot', 'btc-usd', params)
# print(len(data))
#
# # subscribe to realtime ohlcv data
# from google.protobuf.json_format import MessageToJson
#
# for response in client.subscribe_ohlcv('cbse', 'spot', 'btc-usd', '1m'):
#     print("Received message %s" % (MessageToJson(response, including_default_value_fields = True)))

import json
import grpc
import kaikosdk
import requests

from kaikosdk                            import sdk_pb2_grpc
from kaikosdk.core                       import instrument_criteria_pb2
from kaikosdk.stream.aggregates_ohlcv_v1 import request_pb2              as pb_ohlcv
from kaikosdk.stream.aggregates_vwap_v1  import request_pb2              as pb_vwap
from kaikosdk.stream.trades_v1           import request_pb2              as pb_trades


def create_restapi_endpoint(data_type, exchange, instrument_class, instrument, region='us'):
    return f'https://{region}.market-api.kaiko.io/v2/data/trades.v1/exchanges/{exchange}/{instrument_class}/{instrument}/{data_type}'

def create_channel(api_key):
    channel_endpoint = 'gateway-v0-grpc.kaiko.ovh'
    credentials = grpc.ssl_channel_credentials(root_certificates=None)
    call_credentials = grpc.access_token_call_credentials(api_key)
    return grpc.secure_channel(channel_endpoint, grpc.composite_channel_credentials(credentials, call_credentials))

def raise_on_error(result):
    if result.get('result', '') == 'error':
        raise Exception(result.get('message', 'unknown'))

def subscribe_channel(channel, stub, request):
    with channel:
        responses = stub.Subscribe(request)
        for response in responses:
            yield response

subscribe_traits = {
    'ohlcv'  : (sdk_pb2_grpc.StreamAggregatesOHLCVServiceV1Stub, pb_ohlcv.StreamAggregatesOHLCVRequestV1),
    'vwap'   : (sdk_pb2_grpc.StreamAggregatesVWAPServiceV1Stub , pb_vwap.StreamAggregatesVWAPRequestV1),
    'trades' : (sdk_pb2_grpc.StreamTradesServiceV1Stub         , pb_trades.StreamTradesRequestV1)
}

class Client:

    def __init__(self, api_key):
        self.api_key = api_key

    def get_restful(self, endpoint, params):
        headers = { "X-Api-Key": self.api_key, "Accept": "application/json" }
        data_all = []
        next_url = endpoint
        while next_url != "":
            response = requests.get(next_url, headers=headers, params=params)
            result = response.json()
            raise_on_error(result)
            data_all.extend(result.get("data", []))
            next_url = result.get("next_url", "")
            params = {} # we just need it at first time

        return data_all

    def subscribe_impl(self, data_type, exchange, instrument_class, code, params={}):
        create_stub, create_request = subscribe_traits[data_type]
        instrument_criteria = instrument_criteria_pb2.InstrumentCriteria(exchange=exchange, instrument_class=instrument_class, code=code)
        channel = create_channel(self.api_key)
        request = create_request(instrument_criteria=instrument_criteria, **params)
        stub = create_stub(channel)
        return subscribe_channel(channel, stub, request)

    def get_aggregation(self, aggregation, exchange, instrument_class, code, params):
        endpoint = create_restapi_endpoint("aggregations", exchange, instrument_class, code) + f'/{aggregation}'
        return self.get_restful(endpoint, params)

    def get_ohlcv(self, exchange, instrument_class, code, params={}):
        return self.get_aggregation('ohlcv', exchange, instrument_class, code, params)

    def get_vwap(self, exchange, instrument_class, code, params={}):
        return self.get_aggregation('vwap', exchange, instrument_class, code, params)

    def get_trades(self, exchange, instrument_class, code, params={}):
        endpoint = create_restapi_endpoint("trades", exchange, instrument_class, code)
        return self.get_restful(endpoint, params)

    def subscribe_ohlcv(self, exchange, instrument_class, code, aggregate='1m'):
        return self.subscribe_impl('ohlcv', exchange, instrument_class, code, {'aggregate': aggregate})

    def subscribe_vwap(self, exchange, instrument_class, code, aggregate='1m'):
        return self.subscribe_impl('vwap', exchange, instrument_class, code, {'aggregate': aggregate})

    def subscribe_trades(self, exchange, instrument_class, code):
        return self.subscribe_impl('trades', exchange, instrument_class, code)

