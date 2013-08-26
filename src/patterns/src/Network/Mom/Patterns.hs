-------------------------------------------------------------------------------
-- |
-- Module     : Network/Mom/Patterns.hs
-- Copyright  : (c) Tobias Schoofs
-- License    : LGPL 
-- Stability  : experimental
-- Portability: portable
--
-- In distributed message-oriented applications,
-- the same communication patterns show up
-- over and over again.
-- This package implements some of these patterns
-- based on the /zeromq/ library.
-- /Patterns/ uses the /zeromq-haskell/ package,
-- but goes beyond in several aspects:
-- 
-- * It uses /conduits/ to stream incoming and
--   outgoing message segments;
--
-- * It defines libraries of basic patterns
--   to enforce coherent use of /zeromq/ sockets;
--
-- * It implements modules for advanced patterns;
--   currently the majordomo pattern (broker) is implemented.
-- 
-- More information on /zeromq/ can be found at
-- <http://www.zeromq.org>.
-------------------------------------------------------------------------------
module Network.Mom.Patterns (

          -- * Patterns
          -- $patterns

          -- * Streams
          -- $streams

          -- | The type module defines some fundamental types:
          module Network.Mom.Patterns.Types,

          -- | The streams module defines a streaming device
          --   and some useful operations:
          module Network.Mom.Patterns.Streams,

          -- * Basic Patterns
          -- $basic

          module Network.Mom.Patterns.Basic,

          -- * Advanced Patterns
          -- $advanced

          )
where

  import Network.Mom.Patterns.Types
  import Network.Mom.Patterns.Streams
  import Network.Mom.Patterns.Basic

  {- $patterns
     Instead of a centralised message broker
     as the main back bone of reliable
     message exchange, zeromq implements
     an advanced socket concept.
     Zeromq sockets are thread-local resources 
     that connect to each other across
     threads, processes and network nodes
     according to certain protocol patterns.
     
     The /Patterns/ package hides details
     about sockets and 
     provides instead higher-level abstractions,
     in particular an event-driven streaming device 
     and a set of basic patterns.
     Streams come in handy when building more complex pattern,
     such as routers, brokers or load balancers.
     Currently included in this version
     is the Majordomo pattern, 
     a service broker and load balancer
     for the /client\/server/ pattern.

     The interfaces provided by the Patterns package
     support the separation of different concerns
     involved with application design.
     Most of the interfaces are higher-order functions
     that accept 
     stream processors and control actions.
     Distributed application components can be built
     by bundling stream processors 
     together to request or provide services, 
     publish or subscribe data or to 
     allocate work to processing nodes.

     Note that, since the patterns package is based on ZMQ,
     applications based on patterns must be linked with the 
     /-threaded/ flag. 
     
  -}

  {- $basic
     Basic patterns are:

     * Server\/Client (a.k.a Request\/Response)
       consisting of a server process that responds to requests
       and client processes that request a service
       and wait for the server response;
   
     * Publish\/Subscribe
       consisting of a publisher process that
       periodically or sporadically publishes data
       and subscribers that receive data
       corresponding to topics, 
       to which they have actually subscribed;

     * Pipeline (a.k.a. Push\/Pull)
       consisting of a /pusher/ process that sends jobs
       down the pipeline
       and worker processes that connect to the pipeline;
       jobs are work-balanced among workers;

     All of these basic patterns consist of two parts
     which can, roughly, be described as a client and a server side.
     Only those sides belonging to the same pattern
     can communicate with each other. 
     Since communication may - and usually does -
     cross processes and network nodes,
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
       can receive messages only from a pusher;

     Note that the /peer-to-peer/ pattern
     defined in /zeromq/ 
     is not provided by the /patterns/ library.
     For this kind of communication the basic
     /zeromq/ package schould be used.
  -}

  {- $streams
     The /patterns/ package uses conduits for message processing.
     All message endpoints create or receive messages as 
     streams of message segments 
     (even if there is only one segment in the message).
     Endpoint like /clients/, /server/ and so on
     use producers, consumers and conduits from the /conduit/ package
     to handle messages.

     There is additionally a streaming device
     that relays messages between compatible services, /i.e./
     Servers and Clients,
     Publishers and Subscribers and
     Pusher and Pullers. 
     A streaming device polls over a list of access points.
     When data is available,
     the an application-defined stream transformer
     is invoked.
     The outgoing stream may be directed to
     one or several access points
     including the source itself.
     Note, however, that the 
     combination of local access point and remote target socket 
     must adhere to the restrictions of possible peer combinations. 

     Streams are mainly thought to implement
     more complex patterns,
     brokers, load balancers, /etc./
     In basic patterns, they are used 
     to implement background processes,
     for instance in the /server/ module.
  -}

  {- $advanced
     The zeromq design 
     encourages to build new complex patterns,
     some of which are described on the zeromq website.
     The main idea, here, is that the design of the middleware
     should not statically impose one topology 
     on all possible communication scenarios;
     therefore, zeromq does not place a broker
     at the very core of its architecture.
     Instead, application components can connect freely
     using the architectural pattern 
     that best solves the problem at hand.

     Advanced communication patterns use
     message exchange protocols and other means,
     even brokers, to make communication reliable and scalable.

     The 'patterns' library aims to provide
     advanced topics as libraries that can be used 
     flexibly to solve concrete application problems.
     Currently, as a proof-of-concept,
     the majordomo pattern is implemented,
     a service broker, providing a central access point
     for clients, load balancing and service discovery.
  -}

