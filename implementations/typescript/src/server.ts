import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";

type ContractRoute = { method: string; path: string };
type Contract = { version: string; routes: ContractRoute[] };

const sourceDirectory = path.dirname(fileURLToPath(import.meta.url));
const contractPath = path.resolve(sourceDirectory, "../../../protocol/http-contract.json");
const contract = JSON.parse(readFileSync(contractPath, "utf8")) as Contract;
const allowedRoutes = new Set(contract.routes.map((route) => route.method + " " + route.path));
const defaultWorkerUrl = "http://127.0.0.1:8765";
const maxBodyBytes = 1024 * 1024;

function readEnvironment(name: string, fallback: string): string {
  const value = process.env[name]?.trim();
  return value || fallback;
}

function parsePort(raw: string, name: string): number {
  const port = Number(raw);
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error(name + " must be an integer between 1 and 65535");
  }
  return port;
}

function writeJson(response: ServerResponse, status: number, value: unknown): void {
  const payload = Buffer.from(JSON.stringify(value));
  response.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "content-length": String(payload.length),
    "cache-control": "no-store"
  });
  response.end(payload);
}

async function readBody(request: IncomingMessage): Promise<Buffer> {
  const chunks: Buffer[] = [];
  let total = 0;
  for await (const chunk of request) {
    const data = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    total += data.length;
    if (total > maxBodyBytes) {
      throw new Error("request body is too large");
    }
    chunks.push(data);
  }
  return Buffer.concat(chunks);
}

function copyResponseHeaders(upstream: Response, response: ServerResponse): void {
  const headers: Record<string, string> = { "x-dsh-gateway": "typescript" };
  for (const name of ["content-type", "cache-control", "x-accel-buffering"]) {
    const value = upstream.headers.get(name);
    if (value) headers[name] = value;
  }
  response.writeHead(upstream.status, headers);
}

async function writeUpstreamBody(upstream: Response, response: ServerResponse): Promise<void> {
  if (upstream.body === null) {
    response.end();
    return;
  }
  const reader = upstream.body.getReader();
  try {
    while (true) {
      const next = await reader.read();
      if (next.done) break;
      if (!response.write(Buffer.from(next.value))) {
        await new Promise<void>((resolve) => response.once("drain", resolve));
      }
    }
  } finally {
    reader.releaseLock();
    response.end();
  }
}

async function proxyRequest(
  request: IncomingMessage,
  response: ServerResponse,
  workerUrl: string
): Promise<void> {
  const method = request.method ?? "GET";
  const requestUrl = new URL(request.url ?? "/", "http://gateway.local");
  const routeKey = method + " " + requestUrl.pathname;
  if (!allowedRoutes.has(routeKey)) {
    writeJson(response, 404, { ok: false, error: { code: "not_found", message: "Unknown endpoint" } });
    return;
  }

  const body = method === "POST" ? await readBody(request) : undefined;
  const headers = new Headers();
  const contentType = request.headers["content-type"];
  if (typeof contentType === "string") headers.set("content-type", contentType);
  const target = new URL(requestUrl.pathname + requestUrl.search, workerUrl).toString();
  const controller = new AbortController();
  request.once("aborted", () => controller.abort());
  const upstream = await fetch(target, { method, headers, body, signal: controller.signal });

  copyResponseHeaders(upstream, response);
  await writeUpstreamBody(upstream, response);
}

export function createGatewayServer(workerUrl = readEnvironment("DSH_WORKER_URL", defaultWorkerUrl)) {
  return createServer((request, response) => {
    void proxyRequest(request, response, workerUrl).catch((error: unknown) => {
      if (response.headersSent) {
        response.destroy(error instanceof Error ? error : undefined);
        return;
      }
      const message = error instanceof Error ? error.message : "gateway request failed";
      writeJson(response, 502, { ok: false, error: { code: "worker_unavailable", message } });
    });
  });
}

export function startGateway(): void {
  const host = readEnvironment("DSH_GATEWAY_HOST", "127.0.0.1");
  const port = parsePort(readEnvironment("DSH_GATEWAY_PORT", "8780"), "DSH_GATEWAY_PORT");
  const workerUrl = readEnvironment("DSH_WORKER_URL", defaultWorkerUrl);
  const server = createGatewayServer(workerUrl);
  server.listen(port, host, () => {
    process.stdout.write("TypeScript DSH gateway listening on http://" + host + ":" + port + "\n");
    process.stdout.write("Worker: " + workerUrl + "\n");
  });
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  startGateway();
}
