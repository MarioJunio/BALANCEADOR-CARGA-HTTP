__author__ = 'MarioJ'

import Balancer
import sys

balancer = Balancer.Balancer(int(sys.argv[1]))
balancer.init_servers()
balancer.load_stacks()
balancer.start()
