-------------------------------------------------------------------------------
-- |
-- Module     : Network/Mom/Patterns/Broker.hs
-- Copyright  : (c) Tobias Schoofs
-- License    : LGPL 
-- Stability  : experimental
-- Portability: non-portable
-- 
-- Majordomo Service Broker 
-------------------------------------------------------------------------------
module Network.Mom.Patterns.Broker (

         -- * The Majordomo Pattern
         -- $majordomo
        
         -- * Majordomo \'Worker\'
         -- $worker

         module Network.Mom.Patterns.Broker.Server,

         -- * Majordomo \'Client\'
         -- $client

         module Network.Mom.Patterns.Broker.Client,

         -- * Majordomo \'Broker\'
         -- $broker

         module Network.Mom.Patterns.Broker.Broker,

         -- * Majordomo common definitions
         -- $common

         module Network.Mom.Patterns.Broker.Common)
where

  import Network.Mom.Patterns.Broker.Server
  import Network.Mom.Patterns.Broker.Client
  import Network.Mom.Patterns.Broker.Broker
  import Network.Mom.Patterns.Broker.Common

  {- $majordomo
     The Majordomo pattern
     defines a communication protocol
     between clients and servers based
     on a service broker that provides

     * A central access point for all services

     * A service infrastructure for services like
       service discovery, configurations, data storage, /etc./

     * Reliable communication based on heart-beats.

     The broker package does not provide a pre-defined borker,
     /i.e./ it does not contain an application.
     Instead, it provides /API/s that can be used
     to build brokers, even within the same process
     as clients and workers. 

     The present module provides a convenience reexport of the three modules,
     making up the majordomo pattern: server, client and broker.
     There is also a \"common\" module with definitions relevant
     for all three parties. 
  -}

  {- $worker
     The server module contains the server-side of the majordomo pattern.
     Majordomo servers look like ordinary servers,
     but use a different protocol for communicating 
     with the broker intervening between clients and servers.
  -}

  {- $client
     The client module contains the client-side of the majordomo pattern.
     Majordomo clients look very similar to ordinary clients,
     but use a different protocol for communicating 
     with the broker intervening between clients and servers.
     The code provided to a majordomo clients, however,
     is exactly the same as the code provided to an ordinary client.
  -}

  {- $broker
     The broker module contains the broker code.
     The broker is implemented by means of streams,
     very similar to servers and clients.
     The functionality provided by the broker module
     can be used to implement a standalone broker
     or an in-process broker linked with its clients and servers.
  -}

  {- $common
     The \"common\" module contains 
     protocol building blocks and
     MDP-specific exceptions.
  -}
