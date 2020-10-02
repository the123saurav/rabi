While writing any test, make sure you shutdown threads in DB(stop) as well
as your own as surefire waits for non-daemon threads to shutdown.