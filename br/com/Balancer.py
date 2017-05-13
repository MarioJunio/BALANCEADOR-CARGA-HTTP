__author__ = 'MarioJ'
from random import randint
import socket
import time
import thread

'''
This class define how the balancer will to interact with clients and server. Balancing the traffic in both servers
registreds on balancer.
'''


class Balancer(object):
    # MAX MESSAGE LENGTH
    BUFFER_LEN = 1024

    # DEFINE THE RETURN CARRIAGE END LINE ON REQUEST
    _CRLF = '\r\n'

    # DEFINE REQUEST METHODS
    _GET = "GET"
    _POST = "POST"

    _LOAD_STACKSNAME = "load stacksname"

    def __init__(self, port):

        self.host = ''

        # PORT TO BALANCER STARTS
        self.port = port
        # SERVERS LIST
        self.servers = []
        # MAP KEY/VALUE TO REGISTER STACKS AND SERVERS. HASH TABLE
        self.stacks = {}
        # INSTANCE SOCKET TO COMMUNICATE
        self.sock = None

    def init_servers(self):
        self.add_server('localhost', 8080)
        self.add_server('localhost', 8081)
        self.add_server('localhost', 8082)

    def start(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((self.host, self.port))
        self.sock.listen(100)

        print "\n%s -> %s\n" % (self.get_time(), "Balancer Started")

        while 1:
            (client_socket, address) = self.sock.accept()

            print "%s -> %s" % (self.get_time(), "Client connected")

            # process request in new thread
            thread.start_new_thread(self.process, (client_socket,))

    def process(self, client):
        message = client.recv(self.BUFFER_LEN)

        (method, resource, version) = self.format_message(message)

        if method == self._GET:

            self.log("# GET")
            self.log(resource + "\n\n")

            try:
                begin = resource.rindex("/")
                end = resource.rindex(".")
            except:
                begin = -1
                end = -1

            name = None

            if begin != -1:

                if end != -1:
                    name = resource[begin:end]
                else:
                    name = resource[begin:]
            else:
                name = "/index.html"

            if name is not None:
                server_id = self.get_hash(name)
                server = self.servers[server_id]

                host, port = self.parse_server_str(server)
                self.make_request_and_response(host, port, client, message)

        elif method == self._POST:
            stackname = self.get_stackname(message)

            # request stackname on server
            self.log(stackname)

            server = self.stacks.get(stackname)

            if server is None:
                server = self.servers[self.get_hash(stackname)]
                self.add_stack(stackname, server)
                self.log("inserting %s at server %s" % (stackname, server))
            else:
                self.log("stack already mapped")

            # get host and port from string server found in list
            host, port = self.parse_server_str(server)
            self.make_request_and_response(host, port, client, message)

    def make_request_and_response(self, host, port, client, message):

        self.log("trying connect %s at port %d\n\n" % (host, port))

        # make request on server
        sock_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_server.connect((host, port))
        # send request to server
        sock_server.send(message)
        # get response from server
        server_response = self.recv(sock_server)
        # close connection with server
        sock_server.close()

        # resend response to client
        client.sendall(server_response)

        # Close connection with client, works done !
        client.close()


    def add_stack(self, stackname, server):
        self.stacks[stackname] = server

    def add_server(self, host, port):
        self.servers.append(host + ":" + str(port))

    def parse_server_str(self, server_str):
        tokens = server_str.split(':')
        return tokens[0], int(tokens[1])

    def get_random(self):
        size = len(self.servers)
        return self.servers[randint(1, size)]

    def get_hash(self, stackname):
        return (ord(stackname[0]) + ord(stackname[len(stackname) - 1])) % len(self.servers)

    def load_stacks(self):

        (host, port) = self.servers[0].split(':')

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, int(port)))

        self.log("loading stacks...")

        sock.send(self._LOAD_STACKSNAME)
        response = self.recv(sock)

        if response:
            stacks = response.split(',')

            for stack in stacks:
                index = self.get_hash(stack)
                # print 'index ', index
                self.add_stack(stack, self.servers[index])


    def format_message(self, message):
        tokens = message.split(self._CRLF)[0].split(' ')
        return tokens[0], tokens[1], float(tokens[2].split('/')[1])

    def get_stackname(self, message):
        parameters = self.get_parameters(message)

        STACK_NOME = "nome"
        STACK_PARAMETER_SEPARATOR = "&"

        index = parameters.index(STACK_NOME) + len(STACK_NOME) + 1

        try:
            endIndex = parameters.index(STACK_PARAMETER_SEPARATOR, index)
        except:
            endIndex = None

        return parameters[index:endIndex] if endIndex is not None else parameters[index:]

    def get_parameters(self, message):
        return message.split(self._CRLF + self._CRLF)[1]

    def get_time(self):
        return time.strftime("%d/%m/%Y as %H:%M:%S")

    def recv(self, socket):

        data = ''
        part = None

        while part != '':
            part = socket.recv(1024)
            data += part

        return data

    def print_stacks(self):

        for key in self.stacks:
            print key, ':', self.stacks[key], '\n'


    def print_servers(self):
        for server in self.servers:
            print server, '\n'

    def log(self, message):
        print "%s -> %s" % (self.get_time(), message)