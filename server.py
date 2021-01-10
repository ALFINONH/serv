import thrift
from thrift.thrift import TProcessor, TApplicationException, TType
from thrift.server import TThreadedServer
from thrift.protocol import TBinaryProtocolFactory
from thrift.transport import TBufferedTransportFactory, TServerSocket

class CustomTProcessor(TProcessor):
    def process_in(self, iprot):
        api, type, seqid = iprot.read_message_begin()
        if api not in self._service.thrift_services:
            iprot.skip(TType.STRUCT)
            iprot.read_message_end()
            return api, seqid, TApplicationException(TApplicationException.UNKNOWN_METHOD), None  # noqa

        args = getattr(self._service, api + "_args")()
        args.read(iprot)
        iprot.read_message_end()
        result = getattr(self._service, api + "_result")()

        # convert kwargs to args
        api_args = [args.thrift_spec[k][1] for k in sorted(args.thrift_spec)]

        # get client IP address
        client_ip, client_port = iprot.trans.sock.getpeername()

        def call():
            f = getattr(self._handler, api)
            return f(*(args.__dict__[k] for k in api_args), client_ip=client_ip)

        return api, seqid, result, call

class PingPongDispatcher:
    def ping(self, param1, param2, client_ip):
        return "pong %s" % client_ip

pingpong_thrift = thrift.load("pingpong.thrift")
processor = CustomTProcessor(pingpong_thrift.PingService, PingPongDispatcher())
server_socket = TServerSocket(host="127.0.0.1", port=12345, client_timeout=10000)
server = TThreadedServer(processor,
                         server_socket,
                         iprot_factory=TBinaryProtocolFactory(),
                         itrans_factory=TBufferedTransportFactory())
server.serve()
