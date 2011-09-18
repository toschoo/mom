import socket
import time
 
PORT   = 61613
BUFLEN = 8193

cmsg = "CONNECTED\n" + \
       "session: 1\n" + \
       "\n" + \
       "\0"

myq = []

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)
server.bind(('', PORT))

def session (con):
  state = 0
  while True:
    message = con.recv(BUFLEN)
    print 'Data: \n%s' % message
    print "\n"
    # print 'Received %i: %i' % (i, len(message))
    if state == 0:
      msg = cmsg
      state = 1
    elif state == 1:
      msg = process(message)
    if len(msg) > 0: 
      con.send(msg)
    else:
      print "Nothing sent"

def process (msg):
  ls = msg.splitlines()
  if ls[0] == "SEND":
    myq.append(getMsg(ls[4]))
    print "QUEUE: \n"
    print myq
    print "\nQUEUE END: \n"
  return "MESSAGE\n" + \
         "destination:/queue/test\n" + \
         "message-id: 1\n\n" + \
         getMsg(ls[4]) + "\x00"
 
def getMsg (line):
  msg = ""
  for c in line:
    if c == '\x00':
      return msg
    else:
      msg += c
     
    
  

if __name__ == "__main__":
  while True:
    server.listen(1)
    (conn, address) = server.accept()
    session(conn)
		
	

