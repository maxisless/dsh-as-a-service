import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer } from "node:http";
import { after, before, test } from "node:test";
import { createGatewayServer } from "../src/server.ts";

const worker = createServer(async (request, response) => {
  if (request.method === "GET" && request.url === "/health") {
    response.setHeader("content-type", "application/json");
    response.end(JSON.stringify({ ok: true, implementation: "python-worker" }));
    return;
  }
  if (request.method === "POST" && request.url === "/chat") {
    let body = "";
    for await (const chunk of request) body += chunk;
    response.setHeader("content-type", "application/json");
    response.end(JSON.stringify({ ok: true, received: JSON.parse(body) }));
    return;
  }
  if (request.method === "POST" && request.url === "/v1/sessions/demo/runs") {
    let body = "";
    for await (const chunk of request) body += chunk;
    response.setHeader("content-type", "application/json");
    response.end(JSON.stringify({
      received: JSON.parse(body),
      authorization: request.headers.authorization,
      idempotency: request.headers["idempotency-key"]
    }));
    return;
  }
  if (request.method === "POST" && request.url === "/v1/agents") {
    let body = "";
    for await (const chunk of request) body += chunk;
    response.setHeader("content-type", "application/json");
    response.end(JSON.stringify({ received: JSON.parse(body), authorization: request.headers.authorization }));
    return;
  }
  if (request.method === "GET" && request.url === "/v1/artifacts/demo") {
    response.setHeader("content-type", "application/octet-stream");
    response.end(JSON.stringify({ authorization: request.headers.authorization }));
    return;
  }
  if (request.method === "POST" && request.url === "/chat/stream") {
    response.writeHead(200, { "content-type": "text/event-stream" });
    response.write("event: session\ndata: {\\\"session_id\\\":\\\"demo\\\"}\n\n");
    response.end("event: done\ndata: {\\\"answer\\\":\\\"ok\\\"}\n\n");
    return;
  }
  response.writeHead(404).end();
});

const gateway = createGatewayServer("http://127.0.0.1:19001");

before(async () => {
  worker.listen(19001, "127.0.0.1");
  gateway.listen(19002, "127.0.0.1");
  await Promise.all([once(worker, "listening"), once(gateway, "listening")]);
});

after(async () => {
  worker.close();
  gateway.close();
  await Promise.all([once(worker, "close"), once(gateway, "close")]);
});

test("forwards health", async () => {
  const response = await fetch("http://127.0.0.1:19002/health");
  const value = await response.json();
  assert.equal(response.status, 200);
  assert.equal(value.implementation, "python-worker");
  assert.equal(response.headers.get("x-dsh-gateway"), "typescript");
});

test("forwards chat JSON unchanged", async () => {
  const response = await fetch("http://127.0.0.1:19002/chat", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ session_id: "demo", message: "hello" })
  });
  const value = await response.json();
  assert.equal(value.received.session_id, "demo");
  assert.equal(value.received.message, "hello");
});

test("forwards SSE frames", async () => {
  const response = await fetch("http://127.0.0.1:19002/chat/stream", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ message: "stream" })
  });
  const text = await response.text();
  assert.equal(response.headers.get("content-type"), "text/event-stream");
  assert.match(text, /event: session/);
  assert.match(text, /event: done/);
});

test("forwards control-plane parameter routes and identity headers", async () => {
  const response = await fetch("http://127.0.0.1:19002/v1/sessions/demo/runs", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      authorization: "Bearer test-token",
      "idempotency-key": "message-1"
    },
    body: JSON.stringify({ message: "hello" })
  });
  const value = await response.json();
  assert.equal(response.status, 200);
  assert.equal(value.received.message, "hello");
  assert.equal(value.authorization, "Bearer test-token");
  assert.equal(value.idempotency, "message-1");
});

test("forwards Agent publication route", async () => {
  const response = await fetch("http://127.0.0.1:19002/v1/agents", {
    method: "POST",
    headers: { "content-type": "application/json", authorization: "Bearer test-token" },
    body: JSON.stringify({ agent_id: "sales", version: "v1", default_model: "deepseek" })
  });
  const value = await response.json();
  assert.equal(response.status, 200);
  assert.equal(value.received.agent_id, "sales");
  assert.equal(value.authorization, "Bearer test-token");
});

test("forwards artifact download identity header", async () => {
  const response = await fetch("http://127.0.0.1:19002/v1/artifacts/demo", {
    headers: { authorization: "Bearer test-token" }
  });
  const value = await response.json();
  assert.equal(response.status, 200);
  assert.equal(value.authorization, "Bearer test-token");
});
