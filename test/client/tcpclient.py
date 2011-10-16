# bom  : 2
# msgid: 4
# len  : 4
# seq  : 4
# total: 4
#---------
#       18
import socket
import sys

SERVER_ADDRESS = '127.0.0.1'
SERVER_PORT = 61613
 
client = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)
client.connect((SERVER_ADDRESS, SERVER_PORT))

if len(sys.argv) == 0:
  msg = "test message"
else:
  msg = sys.argv[1]

print 'Sending message: %s' % msg 
client.send (msg)
 
client.close()

