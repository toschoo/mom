-------------------------------------------------------------------------------
-- |
-- Module     : Network/Mom/Patterns.hs
-- Copyright  : (c) Tobias Schoofs
-- License    : LGPL 
-- Stability  : experimental
-- Portability: portable
--
-- This package implements communication patterns
-- that are often used in distributed applications.
-- The package implements a set of basic patterns
-- and a device to connect basic patterns through
-- routers, brokers, load balancers, /etc./ 
-- The package is based on the /zeromq/ library,
-- but, in some cases, deviates from the /zeromq/ terminology.
-- More information on /zeromq/ can be found at
-- <http://www.zeromq.org>.
-------------------------------------------------------------------------------
module Network.Mom.Patterns (
          -- * Patterns
          -- $patterns

          -- * Basic Patterns
          -- $basic

          module Network.Mom.Patterns.Basic,

          -- * Devices
          -- $device

          module Network.Mom.Patterns.Device,

          -- * Enumerators
          -- $enumerator

          module Network.Mom.Patterns.Enumerator
          -- * Useful Helpers
          )
where

  import Network.Mom.Patterns.Basic
  import Network.Mom.Patterns.Device
  import Network.Mom.Patterns.Enumerator

  {- $patterns
     Instead of a centralised message broker
     as the main back bone of reliable
     message exchange, zeromq implements
     an advanced socket concept.
     zeromq sockets are thread-local resources 
     that connect to each other across
     threads, processes and network nodes
     according to certain protocol patterns.
     
     The Patterns package hides details
     about sockets and instead 
     provides high-level abstractions
     that are based on a set of basic patterns
     and a /device/ to connect patterns
     with each other by means of
     routers, brokers, load balancers, /etc./
  -}

  {- $basic
     Basic patterns are:

     * Server\/Client (a.k.a Request\/Response)
       consisting of a server process that waits for requests
       and creates a response to a request
       and client processes that request a service
       and wait for the server response;
   
     * Publish\/Subscribe
       consisting of a publisher process that
       periodically or sporadically publishes data
       and subscribers that receive these data
       and select those data that correspond to a topic 
       to which the subscriber has actually subscribed;

     * Pipeline (a.k.a. Push\/Pull)
       consisting of a /pusher/ process that sends jobs
       down the pipeline
       and worker processes that connect to the pipeline;
       jobs are work-balanced among workers;

     * Exclusive Pair (a.k.a. Peer-to-Peer)
       consisting of two peer processes 
       that freely exchange messages between each other.

     All of these basic patterns consist of two parts
     that, roughly, can be described as a client and a server side.
     Only clients and servers of the same pattern
     can communicate with each other. 
     Since communication may - and usually is -
     across processes and network nodes,
     there is no way to enforce the correct combination
     by means of the type system. 
     The programmer has to take care
     that programs pair up correctly.

     The patterns hide details of the underlying  
     communication protocol and, hence,
     guarantee that the protocol is used correctly.
     The implementation, in particular,
     adheres to the following principles: 

     * a client must send a request to a server 
       before it can receive messages (from this server)
 
     * a server must receive a request from a client 
       before it can send messages back to this client;

     * a publisher cannot receive messages and
       can send messages only to subscribers;

     * a subscriber cannot send messages and
       can receive messages only from a publisher;

     * a pusher cannot receive messages and
       can send message only to a puller;

     * a puller cannot send messages and
       can only receive messages from a pipe;

     * peers can exchange messages only with other peers.
  -}

  {- $device
     Devices relay messages between compatible services, /i.e./
     Servers and Clients,
     Publishers and Subscribers,
     Pipes and Pullers and
     Peers and Peers.

     A device polls over a list of access points;
     the list is passed in when the device is started, but
     the application may, at any time,
     add or remove access points.
     When data is available,
     the device activates a stream transformer
     defined by the application
     that transforms the incoming data stream
     into an outgoing data stream.
     The application decides also
     where the outgoing stream is directed to.
     This may be one or more access points
     including the source itself.
     Note, however, that the basic patterns
     restrict the possible combinations. 
     
     Devices are more general than patterns
     and could even be used to simulate basic patterns,
     which may be usefull in some situations.
     It is preferrable, however, to use basic patterns instead of devices
     where ever possible.
     The main purpose of devices
     is to link topologies for 
     load-balancing, routing or scaling;
     they can be seen as a kind of smart software switches
     connecting basic patterns.
  -}

  {- $enumerator
     Basic patterns and devices exchange messages.
     Messages are composed of segments;
     A message may consist of one or many segments.
     The underlying library guarantees that
     a message, consisting of many segments,
     is sent and received as a whole, 
     /i.e./ all segments are sent and received
     or the message as a whole is not sent or
     received at all.
     
     Messages may be segmented into 
     parts with different functional purpose, 
     such as an envelope and a body;
     segments may also be used to 
     split a message into single elements of 
     the same type, /e.g./ the rows of a huge 
     result set of a dababase query.
     
     To uniform the message handling,
     patterns and devices treat all messages
     as message streams. Streams are processed
     using /enumerator/s (to create streams)
     and /iteratee/s (to receive streams).
     One half of the stream processing
     is implemented by application code,
     /e.g./, how a publisher creates its outgoing stream
             is defined by an application-specific enumerator;
             the stream is then sent to subscribers by
             a package-implemented standard iteratee.
             Subscribers, on the other hand, receive
             the stream by means of an internal enumerator
             and call an application-defined iteratee.

     The Enumerator module provides generic
     enumerators (called /fetchers/) and
     iteratees   (called /dumps/)
     that handle common stream patterns,
     such as single segment messages, 
     messages with a fixed size of messages,
     streams generated by lists,
     transformation into strings, lists, monoids, /etc./
 
  -}

