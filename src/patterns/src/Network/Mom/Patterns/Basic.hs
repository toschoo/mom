-------------------------------------------------------------------------------
-- |
-- Module     : Network/Mom/Patterns/Basic.hs
-- Copyright  : (c) Tobias Schoofs
-- License    : LGPL 
-- Stability  : experimental
-- Portability: non-portable
-- 
-- Basic communication patterns
-------------------------------------------------------------------------------
module Network.Mom.Patterns.Basic (

                -- * Basic communication patterns
                -- $basic

                -- * Client Server
                -- $cliser

                module Network.Mom.Patterns.Basic.Client,
                module Network.Mom.Patterns.Basic.Server,

                -- * Pub Sub
                -- $pubsub

                module Network.Mom.Patterns.Basic.Publisher,
                module Network.Mom.Patterns.Basic.Subscriber,

                -- * Pipeline
                -- $pipe
                module Network.Mom.Patterns.Basic.Pusher,
                module Network.Mom.Patterns.Basic.Puller)

where

  import           Network.Mom.Patterns.Basic.Client
  import           Network.Mom.Patterns.Basic.Server
  import           Network.Mom.Patterns.Basic.Publisher
  import           Network.Mom.Patterns.Basic.Subscriber
  import           Network.Mom.Patterns.Basic.Pusher
  import           Network.Mom.Patterns.Basic.Puller

  {- $basic
      The Basic module provides the basic communication patterns
      defined in the zeromq library.
      The Basic module is just a convenience reexport of 
      the single modules containing the patterns.
      The basic functionality is split into small modules,
      since usually one application module will only need
      one side of a communication pattern. For the case,
      an application needs several patterns at the same location,
      the basic module eases life providing a single import
      for all patterns.
  -}

  {- $cliser
     Clients request a service from a server.
     When a server receives a client request,
     it sends a response to this client.
     This implies that clients have to start the communication
     by sending a request.
     Usually, many clients connect to one server.
  -}

  {- $pubsub
     Publishers issue data under some topic,
     to which interested parties can subscribe.
     Subscribers do not need to request data explicitly,
     they receive the newest updates when available,
     once they have subscribed to the topic.
     Usually, many subscribers connect to a publisher.
  -}

  {- $pipe
     Pushers and pullers form a pipeline
     to send work packages downstreams;
     pushers can only send data,
     which represent the work to be done,
     pullers can only receive data.
     Usually, the pusher binds the address,
     to which many pullers connect.
  -}

