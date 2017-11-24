What
====

Senderbender v1 is just a queueing SMPP (SMS) proxy.

Use case: application needs to deliver SMS notifications to users.
Old solution: long-running SMPP job in task queue.
New solution: quick roundtrip to senderbender right inside request handler.


Install
=======

`go get -u github.com/temoto/senderbender`


Future features
===============

* route messages to different SMPP servers
* delay (avoid night delivery)
* check delivery status
* other methods Whatsapp, Telegram, push notification, etc
