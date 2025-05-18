import { Pool } from "pg";
import { config } from "../config";

// Define a type for batch items
export interface BatchItem {
  topic: string;
  ltp: number;
  indexName?: string;
  type?: string;
  strike?: number;
}

// Initialize database connection pool
let pool: Pool;
let dataBatch: BatchItem[] = [];
let batchTimer: NodeJS.Timeout | null = null;

// Cache topic IDs to avoid repeated lookups
const topicCache = new Map<string, number>();

export function createPool(): Pool {
  return new Pool({
    host: config.db.host,
    port: config.db.port,
    user: config.db.user,
    password: config.db.password,
    database: config.db.database,
  });
}

export function initialize(dbPool: Pool) {
  pool = dbPool;
  console.log("Database initialized");

  // TODO: Preload topic cache from database
}

export async function getTopicId(
  topicName: string,
  indexName?: string,
  type?: string,
  strike?: number
): Promise<number> {
  // TODO: Implement this function
  // 1. Check if topic exists in cache
  // 2. If not in cache, check if it exists in database
  // 3. If not in database, insert it
  // 4. Return topic_id

  return 0; // Placeholder
}

export function saveToDatabase(
  topic: string,
  ltp: number,
  indexName?: string,
  type?: string,
  strike?: number
) {
  // TODO: Implement this function
  // 1. Add item to batch
  // 2. If batch timer is not running, start it
  // 3. If batch size reaches threshold, flush batch

  console.log(`Saving to database: ${topic}, LTP: ${ltp}`);
}

export async function flushBatch() {
  // TODO: Implement this function
  // 1. Clear timer
  // 2. If batch is empty, return
  // 3. Process batch items (get topic IDs)
  // 4. Insert data in a transaction
  // 5. Reset batch

  console.log("Flushing batch to database");
}

export async function cleanupDatabase() {
  // Flush any remaining items in the batch
  if (dataBatch.length > 0) {
    await flushBatch();
  }

  // Close the database pool
  if (pool) {
    await pool.end();
  }

  console.log("Database cleanup completed");
}
