The Shutdown() call marks the shuttingDown flag
and then spawns a daemon thread to: 
- checkpoint L<sub>1</sub> and then L<sub>0</sub> files.
- complete the future so that caller knows it.
- marks state as Terminated.