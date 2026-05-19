import { Database } from "bun:sqlite"
import { repairTelemetrySearchIndexes } from "./TelemetryStore.js"

type RepairWorkerMessage =
	| { readonly type: "done" }
	| { readonly type: "error"; readonly message: string }

const send = (message: RepairWorkerMessage) => {
	postMessage(message)
	;(globalThis as typeof globalThis & { close?: () => void }).close?.()
}

const repair = async (databasePath: string) => {
	try {
		await Bun.sleep(1000)
		const db = new Database(databasePath, { create: true, readonly: false })
		try {
			db.exec(`PRAGMA busy_timeout = 15000;`)
			repairTelemetrySearchIndexes(db)
			send({ type: "done" })
		} finally {
			db.close()
		}
	} catch (error) {
		send({ type: "error", message: error instanceof Error ? error.message : String(error) })
	}
}

addEventListener("message", (event: MessageEvent<{ readonly databasePath: string }>) => {
	void repair(event.data.databasePath)
})
