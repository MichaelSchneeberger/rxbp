Real-time data processing
-------------------------
(08.10.2019)

Real-time data processing is used when a device reads data from 
hardware sensors over some protocol. A sensor can go from a simple
temperature sensor to a photovoltaic converter that exposes the
current power production. After processing, the data is either saved
locally on disk, or send to some external node. The real-time
data processing that we are going to investigate in this context is 
the processing that happens right after reading the raw data from 
the sensors until it is saved or forwarded to another node.

There are two ways to do the data processing. Either, the data is first
buffered in memory and then processed (imperative way), or the data is
processed as it is measured (reactive way). Which one to choose, 
depends entirely on the application. If the time interval we are
interested in the measured data is finite and not too long, we might
be better off buffering the data first. If there are some real-time
requirement that speify some response time, then the real-time approach
is more appropriate. But in any case, we need some sort of framework
that let's us scale our application to the desired size. For this talk,
wer are only consider the real-time data processing.



Here are 5 scenarios, where real-time data processing needs to be 
performed:

**1. Difference between measured values and some calculated estimation** 


