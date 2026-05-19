import { Database } from "bun:sqlite"
import { afterAll, beforeAll, describe, expect, it } from "bun:test"
import { copyFileSync, mkdtempSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { Effect, ManagedRuntime, References } from "effect"
import { attributeFiltersFromArgs, attributeContainsFiltersFromArgs, isAttributeFilterToken, isAttributeContainsToken } from "./queryFilters.js"

const runTelemetryStoreStartup = async (databasePath: string) => {
	const script = `
		import { Database } from "bun:sqlite"
		import { Effect, ManagedRuntime, References } from "effect"
		const { TelemetryStore, makeTelemetryStoreLayer } = await import("./src/services/TelemetryStore.ts")
		const runtime = ManagedRuntime.make(makeTelemetryStoreLayer({ readonly: false, runRetention: false }))
		const waitForRepair = async () => {
			const deadline = Date.now() + 10_000
			while (Date.now() < deadline) {
				const db = new Database(${JSON.stringify(databasePath)}, { readonly: true })
				try {
					const operationMarker = db.query("SELECT value FROM telemetry_store_meta WHERE key = 'span_operation_search_index'").get()
					const aiTextMarker = db.query("SELECT value FROM telemetry_store_meta WHERE key = 'ai_text_search_index'").get()
					const logBodyMarker = db.query("SELECT value FROM telemetry_store_meta WHERE key = 'log_body_search_index'").get()
					if (operationMarker && aiTextMarker && logBodyMarker) return
				} catch {
					// The writer may still be finishing schema setup.
				} finally {
					db.close()
				}
				await Bun.sleep(100)
			}
			throw new Error("timed out waiting for search index repair")
		}
		try {
			await runtime.runPromise(
				Effect.flatMap(TelemetryStore, () => Effect.void).pipe(
					Effect.provideService(References.MinimumLogLevel, "None"),
				),
			)
			await waitForRepair()
		} finally {
			await runtime.dispose().catch(() => undefined)
		}
	`
	const proc = Bun.spawn(["bun", "--eval", script], {
		cwd: process.cwd(),
		env: {
			...process.env,
			MOTEL_OTEL_DB_PATH: databasePath,
			MOTEL_OTEL_RETENTION_HOURS: "24",
		},
		stdout: "pipe",
		stderr: "pipe",
	})
	const [stdout, stderr, exitCode] = await Promise.all([
		new Response(proc.stdout).text(),
		new Response(proc.stderr).text(),
		proc.exited,
	])
	if (exitCode !== 0) {
		throw new Error(`telemetry store startup failed (${exitCode})\n${stdout}\n${stderr}`)
	}
}

describe("motel telemetry store", () => {
	const tempDir = mkdtempSync(join(tmpdir(), "motel-test-"))
	const dbPath = join(tempDir, "telemetry.sqlite")
	let storeRuntime: Awaited<typeof import("./runtime.ts")>["storeRuntime"]
	let TelemetryStore: Awaited<typeof import("./services/TelemetryStore.ts")>["TelemetryStore"]
	let makeTelemetryStoreLayer: Awaited<typeof import("./services/TelemetryStore.ts")>["makeTelemetryStoreLayer"]
	let motelOpenApiSpec: Awaited<typeof import("./httpApi.ts")>["motelOpenApiSpec"]

	beforeAll(async () => {
		process.env.MOTEL_OTEL_DB_PATH = dbPath
		process.env.MOTEL_OTEL_RETENTION_HOURS = "24"
		const suffix = `?test=${Date.now()}`
		;({ storeRuntime } = await import(`./runtime.ts${suffix}`))
		;({ TelemetryStore, makeTelemetryStoreLayer } = await import(`./services/TelemetryStore.ts${suffix}`))
		;({ motelOpenApiSpec } = await import(`./httpApi.ts${suffix}`))

		const nowNanos = BigInt(Date.now()) * 1_000_000n
		const oneSecond = 1_000_000_000n

		const ingest = Effect.flatMap(TelemetryStore.asEffect(), (store) =>
				Effect.flatMap(
					store.ingestTraces({
					resourceSpans: [
						{
							resource: {
								attributes: [
									{ key: "service.name", value: { stringValue: "test-api" } },
									{ key: "deployment.environment.name", value: { stringValue: "local" } },
								],
							},
							scopeSpans: [
								{
									scope: { name: "test-scope" },
									spans: [
										{
											traceId: "trace-1",
											spanId: "root-1",
											name: "SessionProcessor.stream",
											kind: 2,
											startTimeUnixNano: String(nowNanos),
											endTimeUnixNano: String(nowNanos + 4n * oneSecond),
											attributes: [
												{ key: "sessionID", value: { stringValue: "session-1" } },
												{ key: "modelID", value: { stringValue: "gpt-5.4" } },
											],
										},
										{
											traceId: "trace-1",
											spanId: "child-1",
											parentSpanId: "root-1",
											name: "tool.call",
											kind: 1,
											startTimeUnixNano: String(nowNanos + oneSecond),
											endTimeUnixNano: String(nowNanos + 2n * oneSecond),
											attributes: [
												{ key: "tool", value: { stringValue: "search" } },
											],
										},
									],
								},
							],
						},
						{
							resource: {
								attributes: [
									{ key: "service.name", value: { stringValue: "test-api" } },
									{ key: "deployment.environment.name", value: { stringValue: "local" } },
								],
							},
							scopeSpans: [
								{
									scope: { name: "test-scope" },
									spans: [
										{
											traceId: "trace-2",
											spanId: "root-2",
											name: "SessionProcessor.stream",
											kind: 2,
											startTimeUnixNano: String(nowNanos + 10n * oneSecond),
											endTimeUnixNano: String(nowNanos + 12n * oneSecond),
											status: { code: 2 },
											attributes: [
												{ key: "sessionID", value: { stringValue: "session-2" } },
												{ key: "modelID", value: { stringValue: "gpt-5.4" } },
											],
										},
									],
								},
							],
						},
					],
					}),
					() => store.ingestLogs({
					resourceLogs: [
						{
							resource: { attributes: [{ key: "service.name", value: { stringValue: "test-api" } }] },
							scopeLogs: [
								{
									scope: { name: "app" },
									logRecords: [
										{
											timeUnixNano: String(nowNanos + 500_000_000n),
											severityText: "INFO",
											traceId: "trace-1",
											spanId: "child-1",
											body: { stringValue: "tool call started" },
											attributes: [{ key: "tool", value: { stringValue: "search" } }],
										},
										{
											timeUnixNano: String(nowNanos + 11n * oneSecond),
											severityText: "ERROR",
											traceId: "trace-2",
											spanId: "root-2",
											body: { stringValue: "stream failed" },
											attributes: [{ key: "tool", value: { stringValue: "none" } }],
										},
									],
								},
							],
						},
					],
				}),
			).pipe(Effect.flatMap(() => Effect.flatMap(TelemetryStore.asEffect(), (store) =>
				store.ingestTraces({
					resourceSpans: [{
						resource: { attributes: [{ key: "service.name", value: { stringValue: "test-api" } }] },
						scopeSpans: [{
							scope: { name: "ai" },
							spans: [
								{
									traceId: "trace-ai",
									spanId: "ai-stream-1",
									name: "ai.streamText",
									kind: 1,
									startTimeUnixNano: String(nowNanos + 20n * oneSecond),
									endTimeUnixNano: String(nowNanos + 40n * oneSecond),
									attributes: [
										{ key: "ai.operationId", value: { stringValue: "ai.streamText" } },
										{ key: "ai.telemetry.functionId", value: { stringValue: "session.llm" } },
										{ key: "ai.model.provider", value: { stringValue: "openai.responses" } },
										{ key: "ai.model.id", value: { stringValue: "gpt-5.4" } },
										{ key: "ai.telemetry.metadata.sessionId", value: { stringValue: "ses_test123" } },
										{ key: "ai.telemetry.metadata.userId", value: { stringValue: "kit" } },
										{ key: "ai.prompt.messages", value: { stringValue: '[{"role":"user","content":"Tell me a joke about programming"}]' } },
										{ key: "ai.response.text", value: { stringValue: "Why do programmers prefer dark mode? Because light attracts bugs!" } },
										{ key: "ai.response.finishReason", value: { stringValue: "stop" } },
										{ key: "ai.usage.inputTokens", value: { stringValue: "150" } },
										{ key: "ai.usage.outputTokens", value: { stringValue: "42" } },
										{ key: "ai.usage.totalTokens", value: { stringValue: "192" } },
										{ key: "ai.usage.cachedInputTokens", value: { stringValue: "100" } },
										{ key: "ai.response.msToFirstChunk", value: { stringValue: "500.5" } },
										{ key: "ai.response.msToFinish", value: { stringValue: "20000" } },
									],
								},
								{
									traceId: "trace-ai",
									spanId: "ai-stream-1-do",
									parentSpanId: "ai-stream-1",
									name: "ai.streamText.doStream",
									kind: 1,
									startTimeUnixNano: String(nowNanos + 21n * oneSecond),
									endTimeUnixNano: String(nowNanos + 39n * oneSecond),
									attributes: [
										{ key: "ai.operationId", value: { stringValue: "ai.streamText.doStream" } },
										{ key: "ai.telemetry.functionId", value: { stringValue: "session.llm" } },
										{ key: "ai.model.provider", value: { stringValue: "openai.responses" } },
										{ key: "ai.model.id", value: { stringValue: "gpt-5.4" } },
										{ key: "ai.telemetry.metadata.sessionId", value: { stringValue: "ses_test123" } },
										{ key: "ai.prompt.messages", value: { stringValue: '[{"role":"user","content":"Tell me a joke about programming"}]' } },
									],
								},
								{
									traceId: "trace-ai",
									spanId: "ai-tool-1",
									parentSpanId: "ai-stream-1",
									name: "ai.toolCall",
									kind: 1,
									startTimeUnixNano: String(nowNanos + 25n * oneSecond),
									endTimeUnixNano: String(nowNanos + 26n * oneSecond),
									attributes: [
										{ key: "ai.toolCall.name", value: { stringValue: "bash" } },
									],
								},
								{
									traceId: "trace-ai",
									spanId: "ai-stream-2",
									name: "ai.generateText",
									kind: 1,
									startTimeUnixNano: String(nowNanos + 50n * oneSecond),
									endTimeUnixNano: String(nowNanos + 55n * oneSecond),
									status: { code: 2 },
									attributes: [
										{ key: "ai.operationId", value: { stringValue: "ai.generateText" } },
										{ key: "ai.telemetry.functionId", value: { stringValue: "session.llm" } },
										{ key: "ai.model.provider", value: { stringValue: "anthropic" } },
										{ key: "ai.model.id", value: { stringValue: "claude-opus-4" } },
										{ key: "ai.telemetry.metadata.sessionId", value: { stringValue: "ses_test456" } },
										{ key: "ai.prompt.messages", value: { stringValue: '[{"role":"user","content":"Summarize this"}]' } },
										{ key: "ai.response.text", value: { stringValue: "Error: rate limited" } },
										{ key: "ai.response.finishReason", value: { stringValue: "error" } },
										{ key: "ai.usage.inputTokens", value: { stringValue: "80" } },
										{ key: "ai.usage.outputTokens", value: { stringValue: "10" } },
										{ key: "ai.usage.totalTokens", value: { stringValue: "90" } },
									],
								},
							],
						}],
					}],
				}),
			))),
		)

		await storeRuntime.runPromise(ingest.pipe(Effect.provideService(References.MinimumLogLevel, "None")))
	})

	afterAll(() => {
		rmSync(tempDir, { recursive: true, force: true })
	})

	it("creates fresh DBs with auto_vacuum=INCREMENTAL", () => {
		// Headline regression test: PRAGMA auto_vacuum is a header-level
		// setting that only takes effect when set BEFORE the first CREATE
		// TABLE, or after a full VACUUM. The previous code set it AFTER
		// schema init, so every motel DB ever created had auto_vacuum=NONE
		// and incremental_vacuum was silently a no-op — the documented
		// mechanism behind the 17GB telemetry.sqlite this test exists to
		// prevent.
		const probe = new Database(dbPath, { readonly: true })
		try {
			const mode = (probe.query(`PRAGMA auto_vacuum`).get() as { auto_vacuum: number }).auto_vacuum
			expect(mode).toBe(2) // 2 = INCREMENTAL
		} finally {
			probe.close()
		}
	})

	it("incremental_vacuum reclaims pages back to the OS after deletes", () => {
		// Proves the full reclaim chain works on the real schema: DELETE
		// → wal_checkpoint → incremental_vacuum → page_count drops. With
		// the previous auto_vacuum=NONE bug, page_count would not change
		// no matter how many incremental_vacuum calls were made.
		// Operate on a copy of the seed DB so we don't destroy state that
		// later tests rely on. Checkpoint first so the copy is consistent.
		const sourceProbe = new Database(dbPath)
		try { sourceProbe.exec(`PRAGMA wal_checkpoint(TRUNCATE);`) } finally { sourceProbe.close() }
		const clonePath = join(tempDir, "telemetry-vacuum-clone.sqlite")
		copyFileSync(dbPath, clonePath)
		const probe = new Database(clonePath)
		try {
			probe.exec(`PRAGMA busy_timeout = 5000;`)

			// Bulk-insert filler so we have enough pages to make truncation
			// observable. SQLite frees pages whole-page-at-a-time, so a tiny
			// fixture (a handful of partial pages) won't yield a measurable
			// page_count delta. 1000 rows × ~600 bytes = ~600KB ≈ 150 pages.
			const stmt = probe.prepare(
				`INSERT INTO spans (trace_id, span_id, parent_span_id, service_name, scope_name, operation_name, kind, start_time_ms, end_time_ms, duration_ms, status, attributes_json, resource_json, events_json) VALUES (?, ?, NULL, 'vac', 'scope', 'op', 'INTERNAL', 0, 0, 0, 'OK', '{}', '{}', '[]')`,
			)
			probe.exec(`BEGIN IMMEDIATE;`)
			const filler = "x".repeat(512)
			for (let i = 0; i < 1000; i++) {
				stmt.run(`v${i.toString().padStart(8, "0")}-${filler.slice(0, 40)}`, `s${i.toString(16).padStart(15, "0")}`)
			}
			probe.exec(`COMMIT;`)

			const pageCountBefore = (probe.query(`PRAGMA page_count`).get() as { page_count: number }).page_count
			expect(pageCountBefore).toBeGreaterThan(50)

			probe.exec(`DELETE FROM spans WHERE service_name = 'vac';`)

			const freelistAfterDelete = (probe.query(`PRAGMA freelist_count`).get() as { freelist_count: number }).freelist_count
			expect(freelistAfterDelete).toBeGreaterThan(0)

			probe.exec(`PRAGMA wal_checkpoint(RESTART);`)
			probe.exec(`PRAGMA incremental_vacuum;`)
			probe.exec(`PRAGMA wal_checkpoint(TRUNCATE);`)

			const pageCountAfter = (probe.query(`PRAGMA page_count`).get() as { page_count: number }).page_count
			const freelistAfter = (probe.query(`PRAGMA freelist_count`).get() as { freelist_count: number }).freelist_count
			expect(pageCountAfter).toBeLessThan(pageCountBefore)
			expect(freelistAfter).toBeLessThan(freelistAfterDelete)
		} finally {
			probe.close()
		}
	})

	it("filters traces by attr.* fields", async () => {
		const result = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore.asEffect(), (store) =>
				store.searchTraces({
					serviceName: "test-api",
					attributeFilters: {
						sessionID: "session-1",
						"deployment.environment.name": "local",
					},
				}),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)

		expect(result).toHaveLength(1)
		expect(result[0]?.traceId).toBe("trace-1")
	})

	it("looks up a span directly by spanId", async () => {
		const result = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore.asEffect(), (store) => store.getSpan("child-1")).pipe(
				Effect.provideService(References.MinimumLogLevel, "None"),
			),
		)

		expect(result?.traceId).toBe("trace-1")
		expect(result?.rootOperationName).toBe("SessionProcessor.stream")
		expect(result?.span.operationName).toBe("tool.call")
		expect(result?.span.depth).toBe(1)
	})

	it("filters logs by spanId", async () => {
		const result = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore.asEffect(), (store) =>
				store.searchLogs({
					spanId: "child-1",
				}),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)

		expect(result).toHaveLength(1)
		expect(result[0]?.body).toBe("tool call started")
	})

	it("searches spans by operation, parent operation, and attr filters", async () => {
		const result = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore.asEffect(), (store) =>
				store.searchSpans({
					serviceName: "test-api",
					operation: "tool.call",
					parentOperation: "SessionProcessor.stream",
					attributeFilters: {
						tool: "search",
					},
				}),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)

		expect(result).toHaveLength(1)
		expect(result[0]?.traceId).toBe("trace-1")
		expect(result[0]?.span.operationName).toBe("tool.call")
		expect(result[0]?.parentOperationName).toBe("SessionProcessor.stream")
	})

	it("lists spans for a trace", async () => {
		const result = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore.asEffect(), (store) => store.listTraceSpans("trace-1")).pipe(
				Effect.provideService(References.MinimumLogLevel, "None"),
			),
		)

		expect(result).toHaveLength(2)
		expect(result[0]?.traceId).toBe("trace-1")
	})

	it("documents the span lookup route in OpenAPI", () => {
		expect(motelOpenApiSpec.paths["/api/spans/{spanId}"]).toBeDefined()
	})

	it("aggregates trace stats by operation", async () => {
		const result = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore.asEffect(), (store) =>
				store.traceStats({
					groupBy: "operation",
					agg: "avg_duration",
					serviceName: "test-api",
				}),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)

		expect(result.length).toBeGreaterThanOrEqual(1)
		const sessionOp = result.find((r) => r.group === "SessionProcessor.stream")
		expect(sessionOp).toBeDefined()
		expect(sessionOp?.count).toBe(2)
		expect(sessionOp?.value).toBe(3000)
	})

	it("aggregates log stats by severity", async () => {
		const result = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore.asEffect(), (store) =>
				store.logStats({
					groupBy: "severity",
					agg: "count",
					serviceName: "test-api",
				}),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)

		expect(result).toHaveLength(2)
		const errorGroup = result.find((r) => r.group === "ERROR")
		const infoGroup = result.find((r) => r.group === "INFO")
		expect(errorGroup?.value).toBe(1)
		expect(infoGroup?.value).toBe(1)
	})

	it("documents the stats routes in OpenAPI", () => {
		expect(motelOpenApiSpec.paths["/api/traces/stats"]).toBeDefined()
		expect(motelOpenApiSpec.paths["/api/logs/stats"]).toBeDefined()
		expect(motelOpenApiSpec.paths["/api/spans/{spanId}/logs"]).toBeDefined()
		expect(motelOpenApiSpec.paths["/api/spans/search"]).toBeDefined()
		expect(motelOpenApiSpec.paths["/api/traces/{traceId}/spans"]).toBeDefined()
	})

	it("lists trace summaries without loading spans", async () => {
		const result = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore.asEffect(), (store) =>
				store.listTraceSummaries(null),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)

		expect(result).toHaveLength(3) // trace-1, trace-2, trace-ai
		// Ordered by start time descending — trace-ai is most recent
		expect(result[0]?.traceId).toBe("trace-ai")
		// Summary fields are correct for the original trace
		const trace1 = result.find((r) => r.traceId === "trace-1")
		expect(trace1?.serviceName).toBe("test-api")
		expect(trace1?.rootOperationName).toBe("SessionProcessor.stream")
		expect(trace1?.spanCount).toBe(2)
		expect(trace1?.durationMs).toBe(4000)
	})

	it("lists trace summaries filtered by service", async () => {
		const result = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore.asEffect(), (store) =>
				store.listTraceSummaries("test-api"),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)

		expect(result).toHaveLength(3) // all traces are test-api
	})

	it("searches trace summaries with status filter", async () => {
		const result = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore.asEffect(), (store) =>
				store.searchTraceSummaries({
					serviceName: "test-api",
					status: "error",
				}),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)

		expect(result).toHaveLength(2) // trace-2 and trace-ai (ai.generateText has error status)
		expect(result.some((r) => r.traceId === "trace-2")).toBe(true)
	})

	it("searches trace summaries with attribute filters", async () => {
		const result = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore.asEffect(), (store) =>
				store.searchTraceSummaries({
					serviceName: "test-api",
					attributeFilters: { sessionID: "session-1" },
				}),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)

		expect(result).toHaveLength(1)
		expect(result[0]?.traceId).toBe("trace-1")
	})

	it("searches trace summaries with operation filter", async () => {
		const result = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore.asEffect(), (store) =>
				store.searchTraceSummaries({
					serviceName: "test-api",
					operation: "tool.call",
				}),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)

		expect(result).toHaveLength(1)
		expect(result[0]?.traceId).toBe("trace-1")
	})

	it("treats AI trace search text without FTS tokens as a no-op", async () => {
		const [unfiltered, punctuationOnly] = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore, (store) =>
				Effect.all([
					store.searchTraceSummaries({ serviceName: "test-api" }),
					store.searchTraceSummaries({ serviceName: "test-api", aiText: "!!! &&&" }),
				]),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)

		expect(punctuationOnly.map((trace) => trace.traceId)).toEqual(unfiltered.map((trace) => trace.traceId))
	})

	it("filters logs by severity", async () => {
		const result = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore.asEffect(), (store) =>
				store.searchLogs({ serviceName: "test-api", severity: "ERROR" }),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)

		expect(result).toHaveLength(1)
		expect(result[0]?.body).toBe("stream failed")
		expect(result[0]?.severityText).toBe("ERROR")
	})

	it("filters logs by severity case-insensitively", async () => {
		const result = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore.asEffect(), (store) =>
				store.searchLogs({ serviceName: "test-api", severity: "error" }),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)

		expect(result).toHaveLength(1)
		expect(result[0]?.severityText).toBe("ERROR")
	})

	it("searches log body case-insensitively", async () => {
		const result = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore.asEffect(), (store) =>
				store.searchLogs({ serviceName: "test-api", body: "STREAM FAILED" }),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)

		expect(result).toHaveLength(1)
		expect(result[0]?.body).toBe("stream failed")
	})

	it("searches spans by traceId", async () => {
		const result = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore.asEffect(), (store) =>
				store.searchSpans({ traceId: "trace-1" }),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)

		expect(result).toHaveLength(2)
		expect(result.every((s) => s.traceId === "trace-1")).toBe(true)
	})

	it("searches spans with attrContains substring filter", async () => {
		const result = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore.asEffect(), (store) =>
				store.searchSpans({
					serviceName: "test-api",
					attributeContainsFilters: { sessionID: "session" },
				}),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)

		// Both root spans have sessionID containing "session"
		expect(result.length).toBeGreaterThanOrEqual(2)
	})

	it("searches spans with attrContains case-insensitively", async () => {
		const result = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore.asEffect(), (store) =>
				store.searchSpans({
					serviceName: "test-api",
					attributeContainsFilters: { modelID: "GPT" },
				}),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)

		// modelID is "gpt-5.4", searching "GPT" should match case-insensitively
		expect(result.length).toBeGreaterThanOrEqual(2)
	})

	it("searches logs with attrContains substring filter", async () => {
		const result = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore.asEffect(), (store) =>
				store.searchLogs({
					serviceName: "test-api",
					attributeContainsFilters: { tool: "sea" },
				}),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)

		expect(result).toHaveLength(1)
		expect(result[0]?.body).toBe("tool call started")
	})

	it("combines severity and body filters", async () => {
		const result = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore.asEffect(), (store) =>
				store.searchLogs({ serviceName: "test-api", severity: "INFO", body: "tool" }),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)

		expect(result).toHaveLength(1)
		expect(result[0]?.body).toBe("tool call started")
	})

	it("computes facet status without N+1 queries", async () => {
		const result = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore.asEffect(), (store) =>
				store.listFacets({
					type: "traces",
					field: "status",
				}),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)

		expect(result).toHaveLength(2)
		const errorFacet = result.find((r) => r.value === "error")
		const okFacet = result.find((r) => r.value === "ok")
		expect(errorFacet?.count).toBe(2) // trace-2 and trace-ai
		expect(okFacet?.count).toBe(1)
	})

	it("computes logStats with SQL aggregation", async () => {
		const result = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore.asEffect(), (store) =>
				store.logStats({
					groupBy: "service",
					agg: "count",
					serviceName: "test-api",
				}),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)

		expect(result).toHaveLength(1)
		expect(result[0]?.group).toBe("test-api")
		expect(result[0]?.value).toBe(2)
	})

	it("computes traceStats count via SQL", async () => {
		const result = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore.asEffect(), (store) =>
				store.traceStats({
					groupBy: "status",
					agg: "count",
					serviceName: "test-api",
				}),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)

		expect(result).toHaveLength(2)
		const errorGroup = result.find((r) => r.group === "error")
		const okGroup = result.find((r) => r.group === "ok")
		expect(errorGroup?.count).toBe(2) // trace-2 and trace-ai
		expect(okGroup?.count).toBe(1)
	})

	it("computes traceStats error_rate via SQL", async () => {
		const result = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore.asEffect(), (store) =>
				store.traceStats({
					groupBy: "service",
					agg: "error_rate",
					serviceName: "test-api",
				}),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)

		expect(result).toHaveLength(1)
		expect(result[0]?.group).toBe("test-api")
		// 2 error traces out of 3 total
		expect(result[0]?.value).toBeCloseTo(2 / 3, 5)
	})

	it("documents the docs routes in OpenAPI", () => {
		expect(motelOpenApiSpec.paths["/api/docs"]).toBeDefined()
		expect(motelOpenApiSpec.paths["/api/docs/{name}"]).toBeDefined()
	})

	it("parses attr filters consistently for CLI-style args", () => {
		expect(isAttributeFilterToken("attr.sessionID=sess_123")).toBe(true)
		expect(isAttributeFilterToken("sessionID=sess_123")).toBe(false)
		expect(attributeFiltersFromArgs(["attr.sessionID=sess_123", "attr.tool=search"])).toEqual({
			sessionID: "sess_123",
			tool: "search",
		})
	})

	it("parses attrContains filters for CLI-style args", () => {
		expect(isAttributeContainsToken("attrContains.ai.prompt=hello world")).toBe(true)
		expect(isAttributeContainsToken("attr.key=exact")).toBe(false)
		expect(attributeContainsFiltersFromArgs(["attrContains.ai.prompt=hello", "attr.exact=match"])).toEqual({
			"ai.prompt": "hello",
		})
	})

	it("attr filters exclude attrContains tokens", () => {
		const mixed = ["attr.key=exact", "attrContains.key=substring"]
		expect(attributeFiltersFromArgs(mixed)).toEqual({ key: "exact" })
		expect(attributeContainsFiltersFromArgs(mixed)).toEqual({ key: "substring" })
	})

	// AI Call tests

	it("searches AI calls and returns compact summaries", async () => {
		const result = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore.asEffect(), (store) =>
				store.searchAiCalls({}),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)

		expect(result).toHaveLength(3) // ai.streamText, ai.toolCall, ai.generateText
		const streamCall = result.find((c) => c.spanId === "ai-stream-1")
		expect(streamCall).toBeDefined()
		expect(streamCall?.operation).toBe("streamText")
		expect(streamCall?.model).toBe("gpt-5.4")
		expect(streamCall?.provider).toBe("openai.responses")
		expect(streamCall?.sessionId).toBe("ses_test123")
		expect(streamCall?.userId).toBe("kit")
		expect(streamCall?.finishReason).toBe("stop")
		expect(streamCall?.promptPreview).toContain("Tell me a joke")
		expect(streamCall?.responsePreview).toContain("dark mode")
		expect(streamCall?.toolCallCount).toBe(1)
		expect(streamCall?.usage?.inputTokens).toBe(150)
		expect(streamCall?.usage?.outputTokens).toBe(42)
	})

	it("dedupes nested doStream spans from AI summaries", async () => {
		const result = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore.asEffect(), (store) =>
				store.searchAiCalls({ sessionId: "ses_test123" }),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)

		expect(result.map((call) => call.spanId)).toEqual(["ai-stream-1"])
		expect(result.map((call) => call.spanId)).not.toContain("ai-stream-1-do")
	})

	it("filters AI calls by model", async () => {
		const result = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore.asEffect(), (store) =>
				store.searchAiCalls({ model: "claude-opus-4" }),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)

		expect(result).toHaveLength(1)
		expect(result[0]?.model).toBe("claude-opus-4")
		expect(result[0]?.status).toBe("error")
	})

	it("filters AI calls by sessionId", async () => {
		const result = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore.asEffect(), (store) =>
				store.searchAiCalls({ sessionId: "ses_test123" }),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)

		expect(result).toHaveLength(1)
		expect(result[0]?.spanId).toBe("ai-stream-1")
	})

	it("searches AI calls by text content", async () => {
		const result = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore.asEffect(), (store) =>
				store.searchAiCalls({ text: "joke about programming" }),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)

		expect(result).toHaveLength(1)
		expect(result[0]?.spanId).toBe("ai-stream-1")
	})

	it("matches AI calls via words in the response text", async () => {
		// Verifies FTS indexes ai.response.text, not just ai.prompt*. The
		// seeded ai-stream-2 has response "Error: rate limited".
		const result = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore.asEffect(), (store) =>
				store.searchAiCalls({ text: "rate limited" }),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)
		expect(result.map((r) => r.spanId)).toContain("ai-stream-2")
	})

	it("matches AI calls case-insensitively and with partial words", async () => {
		// unicode61 tokenizer is case-insensitive by default; prefix `*`
		// handles partial terms like `"PROG"` matching `"programming"`.
		const result = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore.asEffect(), (store) =>
				store.searchAiCalls({ text: "PROG" }),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)
		expect(result.map((r) => r.spanId)).toContain("ai-stream-1")
	})

	it("ignores FTS special characters without syntax errors", async () => {
		// FTS5 treats `"`, `*`, `-`, `:` as operators; toFtsQuery must
		// strip them so raw user input never crashes the query.
		const result = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore.asEffect(), (store) =>
				store.searchAiCalls({ text: `"joke" - about:programming*` }),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)
		expect(result.map((r) => r.spanId)).toContain("ai-stream-1")
	})

	it("treats AI call search text without FTS tokens as a no-op", async () => {
		const [unfiltered, punctuationOnly] = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore, (store) =>
				Effect.all([
					store.searchAiCalls({}),
					store.searchAiCalls({ text: "!!! &&&" }),
				]),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)

		expect(punctuationOnly.map((call) => call.spanId)).toEqual(unfiltered.map((call) => call.spanId))
	})

	it("filters AI calls by operation type", async () => {
		const result = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore.asEffect(), (store) =>
				store.searchAiCalls({ operation: "generateText" }),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)

		expect(result).toHaveLength(1)
		expect(result[0]?.operation).toBe("generateText")
	})

	it("gets AI call detail with full payloads", async () => {
		const result = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore.asEffect(), (store) =>
				store.getAiCall("ai-stream-1"),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)

		expect(result).not.toBeNull()
		expect(result?.model).toBe("gpt-5.4")
		expect(result?.promptMessages).toBeDefined()
		expect(result?.responseText).toContain("dark mode")
		expect(result?.toolCalls).toHaveLength(1)
		expect(result?.toolCalls[0]?.name).toBe("bash")
		expect(result?.usage?.inputTokens).toBe(150)
		expect(result?.timing.msToFirstChunk).toBe(500.5)
		expect(result?.timing.msToFinish).toBe(20000)
	})

	it("returns null for non-AI span in getAiCall", async () => {
		const result = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore.asEffect(), (store) =>
				store.getAiCall("root-1"),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)

		expect(result).toBeNull()
	})

	it("aggregates AI call stats by model", async () => {
		const result = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore.asEffect(), (store) =>
				store.aiCallStats({ groupBy: "model", agg: "count" }),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)

		expect(result.length).toBeGreaterThanOrEqual(2)
		const gpt = result.find((r) => r.group === "gpt-5.4")
		const claude = result.find((r) => r.group === "claude-opus-4")
		expect(gpt?.count).toBeGreaterThanOrEqual(1)
		expect(claude?.count).toBe(1)
	})

	it("aggregates AI call stats by status", async () => {
		const result = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore.asEffect(), (store) =>
				store.aiCallStats({ groupBy: "status", agg: "count" }),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)

		const okGroup = result.find((r) => r.group === "ok")
		const errorGroup = result.find((r) => r.group === "error")
		expect(okGroup).toBeDefined()
		expect(errorGroup).toBeDefined()
		expect(errorGroup?.count).toBe(1)
	})

	it("writer startup rebuilds search indexes from canonical tables", async () => {
		const migrationPath = join(tempDir, "search-index-migration.sqlite")
		const probe = new Database(migrationPath)
		try {
			probe.exec(`
				CREATE TABLE spans (
					trace_id TEXT NOT NULL,
					span_id TEXT NOT NULL,
					parent_span_id TEXT,
					service_name TEXT NOT NULL,
					scope_name TEXT,
					operation_name TEXT NOT NULL,
					kind TEXT,
					start_time_ms INTEGER NOT NULL,
					end_time_ms INTEGER NOT NULL,
					duration_ms REAL NOT NULL,
					status TEXT NOT NULL,
					attributes_json TEXT NOT NULL,
					resource_json TEXT NOT NULL,
					events_json TEXT NOT NULL,
					PRIMARY KEY (trace_id, span_id)
				);

				CREATE TABLE span_attributes (
					trace_id TEXT NOT NULL,
					span_id TEXT NOT NULL,
					key TEXT NOT NULL,
					value TEXT NOT NULL,
					PRIMARY KEY (trace_id, span_id, key)
				);

				CREATE TABLE logs (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					trace_id TEXT,
					span_id TEXT,
					service_name TEXT NOT NULL,
					scope_name TEXT,
					severity_text TEXT NOT NULL,
					timestamp_ms INTEGER NOT NULL,
					body TEXT NOT NULL,
					attributes_json TEXT NOT NULL,
					resource_json TEXT NOT NULL
				);

				CREATE VIRTUAL TABLE span_operation_fts USING fts5(
					trace_id UNINDEXED,
					span_id UNINDEXED,
					operation_name,
					tokenize='unicode61'
				);

				CREATE VIRTUAL TABLE span_attr_fts USING fts5(
					value,
					content='span_attributes',
					content_rowid='rowid',
					tokenize='unicode61 remove_diacritics 2'
				);

				CREATE VIRTUAL TABLE log_body_fts USING fts5(
					log_id UNINDEXED,
					body,
					tokenize='unicode61'
				);

				CREATE TRIGGER span_attr_fts_ai AFTER INSERT ON span_attributes
				BEGIN
					INSERT INTO span_attr_fts(rowid, value) VALUES (new.rowid, new.value);
				END;

				INSERT INTO spans(
					trace_id, span_id, parent_span_id, service_name, scope_name, operation_name, kind,
					start_time_ms, end_time_ms, duration_ms, status, attributes_json, resource_json, events_json
				)
				VALUES
					('trace-migrate', 'span-migrate', NULL, 'migration-api', 'migration-test', 'newop', 'INTERNAL', 1, 2, 1, 'ok', '{}', '{}', '[]'),
					('trace-stale-only', 'span-stale-only', NULL, 'migration-api', 'migration-test', 'createdop', 'INTERNAL', 3, 4, 1, 'ok', '{}', '{}', '[]');

				INSERT INTO span_operation_fts(rowid, trace_id, span_id, operation_name)
				VALUES
					(1, 'trace-migrate', 'span-migrate', 'oldop'),
					(2, 'trace-migrate', 'span-migrate', 'newop'),
					(3, 'trace-stale-only', 'span-stale-only', 'staleop');

				INSERT INTO span_attributes(trace_id, span_id, key, value)
				VALUES
					('trace-ai-migrate', 'span-ai-migrate', 'ai.response.text', 'legacy searchable answer'),
					('trace-ai-migrate', 'span-ai-migrate', 'http.method', 'legacy searchable answer');

				INSERT INTO span_attr_fts(rowid, value)
				SELECT rowid, value FROM span_attributes;

				INSERT INTO logs(id, trace_id, span_id, service_name, scope_name, severity_text, timestamp_ms, body, attributes_json, resource_json)
				VALUES (1, 'trace-log-migrate', 'span-log-migrate', 'migration-api', 'migration-test', 'INFO', 5, 'fresh log body alpha gap beta', '{}', '{}');

				INSERT INTO log_body_fts(rowid, log_id, body)
				VALUES (42, 1, 'stale log body');
			`)

			const externalContentCount = (probe.query(`SELECT COUNT(*) AS c FROM span_attr_fts`).get() as { c: number }).c
			expect(externalContentCount).toBe(2)
			probe.close()

			await runTelemetryStoreStartup(migrationPath)

			const repaired = new Database(migrationPath)
			try {
				const ftsCount = (repaired.query(`SELECT COUNT(*) AS c FROM span_operation_fts`).get() as { c: number }).c

				const searchOperations = (operation: string) =>
					repaired.query(`
						SELECT s.operation_name
						FROM span_operation_fts
						JOIN span_operation_index AS soi
							ON soi.fts_rowid = span_operation_fts.rowid
						JOIN spans AS s
							ON s.trace_id = soi.trace_id
							AND s.span_id = soi.span_id
						WHERE span_operation_fts MATCH ?
						ORDER BY s.operation_name
					`).all(operation) as Array<{ operation_name: string }>

				const matches = repaired.query(`
					SELECT sa.key
					FROM span_attr_fts fts
					JOIN span_attributes sa ON sa.rowid = fts.rowid
					WHERE fts.value MATCH ?
					ORDER BY sa.key
				`).all("legacy") as Array<{ key: string }>
				const searchLogBodies = (body: string) =>
					repaired.query(`
						SELECT logs.body
						FROM log_body_fts
						JOIN logs ON logs.id = CAST(log_body_fts.log_id AS INTEGER)
						WHERE log_body_fts MATCH ?
						ORDER BY logs.body
					`).all(body) as Array<{ body: string }>
				const operationMarker = repaired.query(`SELECT value FROM telemetry_store_meta WHERE key = 'span_operation_search_index'`).get()
				const aiTextMarker = repaired.query(`SELECT value FROM telemetry_store_meta WHERE key = 'ai_text_search_index'`).get()
				const logBodyMarker = repaired.query(`SELECT value FROM telemetry_store_meta WHERE key = 'log_body_search_index'`).get()
				const indexCount = (repaired.query(`SELECT COUNT(*) AS c FROM span_attr_fts_index`).get() as { c: number }).c
				const triggerCount = (repaired.query(`
					SELECT COUNT(*) AS c
					FROM sqlite_master
					WHERE type = 'trigger'
						AND name IN ('span_attr_fts_ai', 'span_attr_fts_ad', 'span_attr_fts_au')
				`).get() as { c: number }).c

				expect(searchOperations("oldop")).toHaveLength(0)
				expect(searchOperations("staleop")).toHaveLength(0)
				expect(ftsCount).toBe(2)
				expect(searchOperations("newop").map((row) => row.operation_name)).toEqual(["newop"])
				expect(searchOperations("createdop").map((row) => row.operation_name)).toEqual(["createdop"])
				expect(operationMarker).not.toBeNull()
				expect(aiTextMarker).not.toBeNull()
				expect(logBodyMarker).not.toBeNull()
				expect(indexCount).toBe(1)
				expect(triggerCount).toBe(0)
				expect(matches.map((row) => row.key)).toEqual(["ai.response.text"])
				expect(searchLogBodies("stale")).toHaveLength(0)
				expect(searchLogBodies("fresh").map((row) => row.body)).toEqual(["fresh log body alpha gap beta"])
			} finally {
				repaired.close()
			}
		} finally {
			try { probe.close() } catch { /* already closed before startup */ }
		}
	})

	it("readonly stores pick up FTS repair markers without restart", async () => {
		const operationMarkerKey = "span_operation_search_index"
		const attrMarkerKey = "ai_text_search_index"
		const logBodyMarkerKey = "log_body_search_index"
		const traceId = "trace-readonly-fts-repair"
		const spanId = "span-readonly-fts-repair"
		const serviceName = "readonly-fts-repair-api"
		const searchableText = "readonly alpha gap beta marker"
		const searchableLogBody = "readonly log alpha gap beta marker"
		const now = Date.now()
		let operationFtsRowid = 0
		let attrRowid = 0
		let logId = 0
		let operationMarkerValue: string | undefined
		let attrMarkerValue: string | undefined
		let logBodyMarkerValue: string | undefined

		const probe = new Database(dbPath)
		try {
			probe.exec(`PRAGMA busy_timeout = 5000;`)
			operationMarkerValue = (probe.query(`SELECT value FROM telemetry_store_meta WHERE key = ?`).get(operationMarkerKey) as { value: string } | null)?.value
			attrMarkerValue = (probe.query(`SELECT value FROM telemetry_store_meta WHERE key = ?`).get(attrMarkerKey) as { value: string } | null)?.value
			logBodyMarkerValue = (probe.query(`SELECT value FROM telemetry_store_meta WHERE key = ?`).get(logBodyMarkerKey) as { value: string } | null)?.value
			expect(operationMarkerValue).toBe("rowid-v1")
			expect(attrMarkerValue).toBeTruthy()
			expect(logBodyMarkerValue).toBe("rowid-v1")
			if (operationMarkerValue == null || attrMarkerValue == null || logBodyMarkerValue == null) {
				throw new Error("expected FTS repair markers to exist before readonly marker test")
			}

			operationFtsRowid = ((probe.query(`SELECT COALESCE(MAX(rowid), 0) + 1000 AS rowid FROM span_operation_fts`).get() as { rowid: number }).rowid)

			probe.query(`
				INSERT OR REPLACE INTO trace_summaries(
					trace_id, service_name, root_operation_name, started_at_ms, ended_at_ms,
					active_span_count, duration_ms, span_count, error_count
				) VALUES (?, ?, ?, ?, ?, 0, 1, 1, 0)
			`).run(traceId, serviceName, searchableText, now, now + 1)
			probe.query(`
				INSERT OR REPLACE INTO spans(
					trace_id, span_id, parent_span_id, service_name, scope_name, operation_name, kind,
					start_time_ms, end_time_ms, duration_ms, status, attributes_json, resource_json, events_json
				) VALUES (?, ?, NULL, ?, 'readonly-test', ?, 'INTERNAL', ?, ?, 1, 'OK', '{}', '{}', '[]')
			`).run(traceId, spanId, serviceName, searchableText, now, now + 1)
			probe.query(`
				INSERT OR REPLACE INTO span_attributes(trace_id, span_id, key, value)
				VALUES (?, ?, 'ai.response.text', ?)
			`).run(traceId, spanId, searchableText)
			const logInsert = probe.query(`
				INSERT INTO logs(trace_id, span_id, service_name, scope_name, severity_text, timestamp_ms, body, attributes_json, resource_json)
				VALUES (?, ?, ?, 'readonly-test', 'INFO', ?, ?, '{}', '{}')
			`).run(traceId, spanId, serviceName, now, searchableLogBody) as { lastInsertRowid: number | bigint }
			logId = Number(logInsert.lastInsertRowid)

			attrRowid = (probe.query(`
				SELECT rowid
				FROM span_attributes
				WHERE trace_id = ? AND span_id = ? AND key = 'ai.response.text'
			`).get(traceId, spanId) as { rowid: number }).rowid

			probe.query(`
				INSERT OR REPLACE INTO span_operation_fts(rowid, trace_id, span_id, operation_name)
				VALUES (?, ?, ?, ?)
			`).run(operationFtsRowid, traceId, spanId, searchableText)
			probe.query(`
				INSERT OR REPLACE INTO span_operation_index(trace_id, span_id, fts_rowid)
				VALUES (?, ?, ?)
			`).run(traceId, spanId, operationFtsRowid)
			probe.query(`INSERT INTO span_attr_fts(rowid, value) VALUES (?, ?)`).run(attrRowid, searchableText)
			probe.query(`INSERT OR IGNORE INTO span_attr_fts_index(rowid) VALUES (?)`).run(attrRowid)
			probe.query(`INSERT INTO log_body_fts(log_id, body) VALUES (?, ?)`).run(logId, searchableLogBody)
			probe.query(`DELETE FROM telemetry_store_meta WHERE key IN (?, ?, ?)`).run(operationMarkerKey, attrMarkerKey, logBodyMarkerKey)

			const readonlyRuntime = ManagedRuntime.make(makeTelemetryStoreLayer({ readonly: true, runRetention: false }))
			try {
				const beforeRepair = await readonlyRuntime.runPromise(
					Effect.flatMap(TelemetryStore, (store) =>
						Effect.all([
							store.searchSpans({ serviceName, operation: "alpha beta" }),
							store.searchTraceSummaries({ serviceName, aiText: "alpha beta" }),
							store.searchLogs({ serviceName, body: "alpha beta" }),
						]),
					).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
				)
				expect(beforeRepair[0]).toHaveLength(0)
				expect(beforeRepair[1]).toHaveLength(0)
				expect(beforeRepair[2]).toHaveLength(0)

				probe.query(`INSERT OR REPLACE INTO telemetry_store_meta(key, value) VALUES (?, ?)`).run(operationMarkerKey, operationMarkerValue)
				probe.query(`INSERT OR REPLACE INTO telemetry_store_meta(key, value) VALUES (?, ?)`).run(attrMarkerKey, attrMarkerValue)
				probe.query(`INSERT OR REPLACE INTO telemetry_store_meta(key, value) VALUES (?, ?)`).run(logBodyMarkerKey, logBodyMarkerValue)

				const afterRepair = await readonlyRuntime.runPromise(
					Effect.flatMap(TelemetryStore, (store) =>
						Effect.all([
							store.searchSpans({ serviceName, operation: "alpha beta" }),
							store.searchTraceSummaries({ serviceName, aiText: "alpha beta" }),
							store.searchLogs({ serviceName, body: "alpha beta" }),
						]),
					).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
				)
				expect(afterRepair[0]).toHaveLength(1)
				expect(afterRepair[0][0]?.span.operationName).toBe(searchableText)
				expect(afterRepair[1]).toHaveLength(1)
				expect(afterRepair[1][0]?.traceId).toBe(traceId)
				expect(afterRepair[2]).toHaveLength(1)
				expect(afterRepair[2][0]?.body).toBe(searchableLogBody)
			} finally {
				await readonlyRuntime.dispose().catch(() => undefined)
			}
		} finally {
			if (attrRowid !== 0) {
				try {
					probe.query(`INSERT INTO span_attr_fts(span_attr_fts, rowid, value) VALUES ('delete', ?, ?)`).run(attrRowid, searchableText)
				} catch {
					// Test cleanup should not mask the assertion failure.
				}
			}
			if (operationFtsRowid !== 0) {
				probe.query(`DELETE FROM span_operation_fts WHERE rowid = ?`).run(operationFtsRowid)
			}
			if (logId !== 0) {
				try {
					probe.query(`DELETE FROM log_body_fts WHERE log_id = ?`).run(logId)
				} catch {
					// Test cleanup should not mask the assertion failure.
				}
			}
			probe.query(`DELETE FROM span_operation_index WHERE trace_id = ? AND span_id = ?`).run(traceId, spanId)
			probe.query(`DELETE FROM span_attr_fts_index WHERE rowid = ?`).run(attrRowid)
			probe.query(`DELETE FROM span_attributes WHERE trace_id = ? AND span_id = ?`).run(traceId, spanId)
			probe.query(`DELETE FROM logs WHERE id = ?`).run(logId)
			probe.query(`DELETE FROM spans WHERE trace_id = ? AND span_id = ?`).run(traceId, spanId)
			probe.query(`DELETE FROM trace_summaries WHERE trace_id = ?`).run(traceId)
			if (operationMarkerValue) {
				probe.query(`INSERT OR REPLACE INTO telemetry_store_meta(key, value) VALUES (?, ?)`).run(operationMarkerKey, operationMarkerValue)
			}
			if (attrMarkerValue) {
				probe.query(`INSERT OR REPLACE INTO telemetry_store_meta(key, value) VALUES (?, ?)`).run(attrMarkerKey, attrMarkerValue)
			}
			if (logBodyMarkerValue) {
				probe.query(`INSERT OR REPLACE INTO telemetry_store_meta(key, value) VALUES (?, ?)`).run(logBodyMarkerKey, logBodyMarkerValue)
			}
			probe.close()
		}
	})

	it("re-ingests spans without stale operation or attribute index rows", async () => {
		const nowNanos = BigInt(Date.now()) * 1_000_000n
		const oneSecond = 1_000_000_000n
		const oldPayload = {
			resourceSpans: [{
				resource: { attributes: [{ key: "service.name", value: { stringValue: "reingest-api" } }] },
				scopeSpans: [{
					scope: { name: "reingest-test" },
					spans: [{
						traceId: "trace-reingest",
						spanId: "span-reingest",
						name: "reingest.old",
						startTimeUnixNano: String(nowNanos),
						endTimeUnixNano: String(nowNanos + oneSecond),
						attributes: [
							{ key: "stale", value: { stringValue: "old" } },
							{ key: "ai.prompt", value: { stringValue: "still indexed stale prompt" } },
							{ key: "ai.response.text", value: { stringValue: "orphaned stale answer" } },
						],
					}],
				}],
			}],
		}
		const newPayload = {
			resourceSpans: [{
				resource: { attributes: [{ key: "service.name", value: { stringValue: "reingest-api" } }] },
				scopeSpans: [{
					scope: { name: "reingest-test" },
					spans: [{
						traceId: "trace-reingest",
						spanId: "span-reingest",
						name: "reingest.new",
						startTimeUnixNano: String(nowNanos),
						endTimeUnixNano: String(nowNanos + 2n * oneSecond),
						attributes: [
							{ key: "fresh", value: { stringValue: "new" } },
							{ key: "ai.prompt", value: { stringValue: "fresh prompt" } },
							{ key: "ai.response.text", value: { stringValue: "fresh answer" } },
						],
					}],
				}],
			}],
		}

		await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore, (store) => store.ingestTraces(oldPayload)).pipe(
				Effect.provideService(References.MinimumLogLevel, "None"),
			),
		)

		const probe = new Database(dbPath)
		try {
			const unindexedAiRow = probe.query(`
				SELECT rowid, value
				FROM span_attributes
				WHERE trace_id = ?
					AND span_id = ?
					AND key = 'ai.response.text'
			`).get("trace-reingest", "span-reingest") as { rowid: number; value: string }
			probe.query(`INSERT INTO span_attr_fts(span_attr_fts, rowid, value) VALUES ('delete', ?, ?)`).run(unindexedAiRow.rowid, unindexedAiRow.value)
			probe.query(`DELETE FROM span_attr_fts_index WHERE rowid = ?`).run(unindexedAiRow.rowid)
			probe.query(`DELETE FROM spans WHERE trace_id = ? AND span_id = ?`).run("trace-reingest", "span-reingest")
			probe.query(`DELETE FROM trace_summaries WHERE trace_id = ?`).run("trace-reingest")
		} finally {
			probe.close()
		}

		await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore, (store) => store.ingestTraces(newPayload)).pipe(
				Effect.provideService(References.MinimumLogLevel, "None"),
			),
		)

		const [oldOperation, staleAttribute, staleAiText, stalePrompt, freshAttribute] = await storeRuntime.runPromise(
			Effect.flatMap(TelemetryStore, (store) =>
				Effect.all([
					store.searchSpans({ serviceName: "reingest-api", operation: "reingest.old" }),
					store.searchSpans({ serviceName: "reingest-api", attributeFilters: { stale: "old" } }),
					store.searchTraceSummaries({ serviceName: "reingest-api", aiText: "orphaned stale" }),
					store.searchTraceSummaries({ serviceName: "reingest-api", aiText: "still indexed stale" }),
					store.searchSpans({ serviceName: "reingest-api", operation: "reingest.new", attributeFilters: { fresh: "new" } }),
				]),
			).pipe(Effect.provideService(References.MinimumLogLevel, "None")),
		)

		expect(oldOperation).toHaveLength(0)
		expect(staleAttribute).toHaveLength(0)
		expect(staleAiText).toHaveLength(0)
		expect(stalePrompt).toHaveLength(0)
		expect(freshAttribute).toHaveLength(1)
		expect(freshAttribute[0]?.span.operationName).toBe("reingest.new")
	})

	it("documents the AI routes in OpenAPI", () => {
		expect(motelOpenApiSpec.paths["/api/ai/calls"]).toBeDefined()
		expect(motelOpenApiSpec.paths["/api/ai/calls/{spanId}"]).toBeDefined()
		expect(motelOpenApiSpec.paths["/api/ai/stats"]).toBeDefined()
	})
})
