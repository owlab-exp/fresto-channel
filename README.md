fresto-channel
==============

This is to develop and maintain messaging channels for fresto components' communication.

Update:
* Two event hubs are created. The hubs receive performance event data and forward to subscribers.
* Two event streamers are created. The streamers receives from the hubs and send data to pullers.
* Two event writers are created. The writers pull data from the streamers and write the data (Thrift objects) on Hadoop storage by using Pail.
