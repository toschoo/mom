__0.3.0__ State Linkage Exception in license

__0.2.0__ The mime package (Codec.MIME) switched from String
          to Text in 0.4. We follow. Thanks, Dave!

__0.1.1__ Dependency for bytestring set to 0.10


__0.1.0__ Major changes:

          - Compliance with Stomp 1.2:

          - header keys and values are now escaped;
            this, in fact, was missing for Stomp 1.1.

          - header keys and values are not trimmed or padded;
            this, as well, should have been done for Stomp 1.1 already.
            Be aware that Stomp 1.0-like message headers
            may fail now, *e.g.*:
            *message : hello world*
            is not the same anymore as
            *message:hello world*

          - carriage return (ascii 13) plus line feed (ascii 10) 
            is now accepted as end-of-line;
            note that stompl never generates carriage return as end-of-line,
            the standard end-of-line remains line feed.

          - the Message frame may have an *ack* header 
            and should have when a message is sent 
            through a queue that requires explicit ack.

          - the mandatory header in the Ack frame is now *id*
            instead of *message-id*. It should correspond to *ack*
            in the message that is ack'd.
            Note that, to ease backward compatibility,
            Ack frames are generated with both: 
            an *id* and a *message-id* header.

          - a Stomp frame was added.
            The Stomp frame has exactly the same format
            as the Connect frame, but it is handled differently
            with respect to escaping: Connect header keys and values 
            are not escaped, Stomp header keys and values, however, are.

__0.0.3__ Major changes:

          - new attribute "ClientId" in Connect frame 
            for compatibility with ActiveMQ; 

          - all commands accept additional headers
            to ease adaptations to broker-specific features. 

__0.0.2__ Minor corrections and documentation

__0.0.1__ Initial Release
