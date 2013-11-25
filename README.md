fresto-channel
==============

This is to develop and maintain event transmission channels for fresto.

Nov 25, 2013 updates:
* All writer related classes are moved to another repository: fresto-datastore.

Sep 21, 2013 Updates:
* Two event hubs are created. The hubs receive performance event data and forward to subscribers.
* Two event streamers are created. The streamers receives from the hubs and send data to pullers.
* Two event writers are created. The writers pull data from the streamers and write the data (Thrift objects) on Hadoop storage by using Pail.
