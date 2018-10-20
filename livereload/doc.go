/*

This package allows you to do automatic (live) reload when source Go files change as a library (i.e. without a need
for external tool).

    import (
        "eventter.io/livereload"
    )

    func main() {
        // Code up to the point of the call to `livereload.New()` must be deterministic.

		master, worker, err := livereload.New(&livereload.Config{
			Live:      true,                // Set false to start process without live reload.
			Address:   "./livereload.sock", // Path to socket for workers to communicate with master.
			Package:   "...",               // Path to this package.
			Listeners: []livereload.ListenerDefinition{{"tcp", ":8080"}},
		})
		if err != nil {
			panic(err)
		}

		// This is a master process.
		if master != nil {
			if err := master.Run(); err != nil {
                panic(err)
            }
            return
		}

		// If `master` is nil, it means this is worker process. Create listener and do work as usual.
		listener, err := worker.Listen(0)
		if err != nil {
			panic(err)
		}
		defer listener.Close()

        // ... Start servers with created listener(s), do actual work ...

        // Report to master that the worker is ready.
        if err := worker.Ready(); err != nil {
            panic(err)
        }

        // You should watch for SIGTERM signal and when it happens, exit gracefully.
    }


How does it work?

Master process binds to given socket, creates listeners defined in configuration and then starts child process with
the same executable, however, with environment variable `LIVERELOAD_MASTER` set. Call to `livereload.New()` detects
presence of `LIVERELOAD_MASTER` variable and returns a worker instance instead of a master. For this to work, the code
up to the point of call to `livereload.New()` must be deterministic. Listener sockets are passed to the child worker
process using file descriptors after stderr, i.e. 3, 4, 5, etc.

Master process also starts watching for all files in package defined by configuration as well as all packages it
depends on (transitively). When a change is detected, packages are rebuilt, new child process is started, and after it
reports ready, old worker is signalled using `SIGTERM`.

If you change `livereload.Config`, all workers get killed, listeners closed, and master restarts itself with freshly
built executable.


Build without live reload support

You probably won't need live reload in production binaries. Add `production` to build tags and the library will be built,
so it can be run only with `Live: false`.

*/
package livereload
