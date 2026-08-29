# TypeScript Gateway

This is a dependency-free Node 22 TypeScript gateway. It owns no Agent memory
and does not start a DSH runtime. It forwards the shared protocol to a Python
Worker, preserving HTTP status codes, JSON response bodies, and SSE chunks.

Run a Python Worker first, then start the gateway:

    DSH_WORKER_URL=http://127.0.0.1:8765 npm start

The gateway defaults to 127.0.0.1:8780. Its intended evolution is to add
authentication, tenant routing, quotas, request logging, and asynchronous job
coordination without changing the Worker protocol.
