# Tatris helper scripts
This directory contains many Tatris operation and maintenance scripts. They will be copied to the tatris image.

When you log into the Tatris container, 99% of the time you will enter the `/home/tatris/logs` directory.
At this point you can easily use `sh ../scripts/pprof/goroutine` to view the active goroutines of Tatris.
