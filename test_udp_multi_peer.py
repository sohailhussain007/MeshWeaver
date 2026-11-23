import asyncio

from meshweaver.udp_server import UDPServerProtocol
from meshweaver.protocol import create_message, parse_message
from meshweaver.peer_protocol import create_hello_message, encode_message
from peer import Peer


def test_multiple_udp_clients_communicate_with_server():

    async def run_test():
        loop = asyncio.get_running_loop()

        server_transport, server_protocol = (
            await loop.create_datagram_endpoint(
                UDPServerProtocol,
                local_addr=("127.0.0.1", 0),
            )
        )

        server_port = server_transport.get_extra_info("sockname")[1]

        class MultiPeerClient(asyncio.DatagramProtocol):

            def __init__(self, peer_name):
                self.peer_name = peer_name
                self.peer = None
                self.transport = None
                self.hello_future = loop.create_future()
                self.pong_future = loop.create_future()

            def connection_made(self, transport):
                self.transport = transport
                local_addr = transport.get_extra_info("sockname")

                self.peer = Peer(
                    peer_id=self.peer_name,
                    host="127.0.0.1",
                    port=local_addr[1],
                )

                hello = create_hello_message(self.peer)
                self.transport.sendto(encode_message(hello))

            def datagram_received(self, data, addr):
                message = parse_message(data.decode("utf-8"))

                if message["type"] == "HELLO_ACK":
                    if not self.hello_future.done():
                        self.hello_future.set_result(message)

                    ping = create_message("PING", {})
                    self.transport.sendto(ping.encode("utf-8"))

                elif message["type"] == "PONG":
                    if not self.pong_future.done():
                        self.pong_future.set_result(message)

        client_transports = []
        clients = []

        try:
            for index in range(3):
                transport, client = await loop.create_datagram_endpoint(
                    lambda i=index: MultiPeerClient(f"node_{i + 1}"),
                    remote_addr=("127.0.0.1", server_port),
                )

                client_transports.append(transport)
                clients.append(client)

            acknowledgements = await asyncio.wait_for(
                asyncio.gather(
                    *(client.hello_future for client in clients)
                ),
                timeout=3,
            )

            assert len(acknowledgements) == 3

            for acknowledgement in acknowledgements:
                assert acknowledgement["data"]["registered"] is True

            pong_responses = await asyncio.wait_for(
                asyncio.gather(
                    *(client.pong_future for client in clients)
                ),
                timeout=3,
            )

            assert len(pong_responses) == 3
            assert all(
                response["type"] == "PONG"
                for response in pong_responses
            )

            registered_peers = server_protocol.peer_registry.get_all_peers()
            assert len(registered_peers) == 3

            registered_ids = {
                peer.peer_id for peer in registered_peers
            }

            assert registered_ids == {
                "node_1",
                "node_2",
                "node_3",
            }

        finally:
            for transport in client_transports:
                transport.close()

            server_transport.close()

    asyncio.run(run_test())
