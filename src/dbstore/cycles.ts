import * as db from './sqlite3storage'
import { cycleDatabase } from '.'
import { P2P } from '@shardeum-foundation/lib-types'
import * as Logger from '../Logger'
import { config } from '../Config'
import { DeSerializeFromJsonString, SerializeToJsonString } from '../utils/serialization'
import { Cycle, DbCycle } from './types'
import { calculateBucketID, CycleCheckpointData } from '../checkpoint/CycleData'
import { cycleCheckpointManager } from '../checkpoint/CycleData'
import * as crypto from 'crypto'
import { safeStringify } from '@shardeum-foundation/lib-types/build/src/utils/functions/stringify'
import { CheckpointType } from '../checkpoint/CheckpointData'

export async function insertCycle(cycle: Cycle): Promise<void> {

  try {
    // Define the table columns based on schema
    const columns = ['cycleMarker', 'counter', 'cycleRecord'];

    // Construct the SQL query with placeholders
    const placeholders = `(${columns.map(() => '?').join(', ')})`;
    const sql = `INSERT OR REPLACE INTO cycles (${columns.join(', ')}) VALUES ${placeholders}`;

    // Map the `cycle` object to match the columns
    const values = columns.map((column) =>
      typeof cycle[column] === 'object'
        ? SerializeToJsonString(cycle[column]) // Serialize objects to JSON
        : cycle[column]
    );

    // Execute the query directly (single-row insert)
    await db.run(cycleDatabase, sql, values);

    console.log('[check-point] insertCycle start')
    const bucketID = calculateBucketID(cycle)
    const checkpointData = new CycleCheckpointData(cycle)
    cycleCheckpointManager.addData(checkpointData, bucketID)
    console.log('[check-point] insertCycle end')
    if (config.VERBOSE) {
      Logger.mainLogger.debug(
        'Successfully inserted Cycle',
        cycle.counter,
        cycle.cycleMarker
      );
    }
  } catch (err) {
    Logger.mainLogger.error(err);
    Logger.mainLogger.error(
      'Unable to insert cycle or it is already stored in the database',
      cycle.counter,
      cycle.cycleMarker
    );
  }
}

//change in cycle; need to update checkpoint
export async function bulkInsertCycles(cycles: Cycle[]): Promise<void> {
  try {
    // First create checkpoints for all cycles
    console.log('[check-point] bulkInsertCycles start')
    for (const cycle of cycles) {
      const checkpointData = new CycleCheckpointData(cycle)
      const bucketID = calculateBucketID(cycle)
      cycleCheckpointManager.addData(checkpointData, bucketID)
    }
    console.log('[check-point] bulkInsertCycles end')
    // Then do the database operation
    const columns = ['cycleMarker', 'counter', 'cycleRecord']

    // Construct the SQL query for bulk insertion with all placeholders
    const placeholders = cycles.map(() => `(${columns.map(() => '?').join(', ')})`).join(', ')
    const sql = `INSERT OR REPLACE INTO cycles (${columns.join(', ')}) VALUES ${placeholders}`

    // Flatten the `cycles` array into a single list of values
    const values = cycles.flatMap((cycle) =>
      columns.map((column) =>
        typeof cycle[column] === 'object'
          ? SerializeToJsonString(cycle[column]) // Serialize objects to JSON
          : cycle[column]
      )
    )

    // Execute the single query for all cycles
    await db.run(cycleDatabase, sql, values)

    if (config.VERBOSE) {
      Logger.mainLogger.debug('Successfully inserted Cycles', cycles.length)
    }
  } catch (err) {
    Logger.mainLogger.error(err);
    Logger.mainLogger.error('Unable to bulk insert Cycles', cycles.length);
  }
}

//change in cycle; need to update checkpoint
export async function updateCycle(marker: string, cycle: Cycle): Promise<void> {
  try {
    // Create a checkpoint before updating
    console.log('[check-point] updateCycle start')
    const checkpointData = new CycleCheckpointData(cycle)
    const bucketID = calculateBucketID(cycle)
    cycleCheckpointManager.addData(checkpointData, bucketID)
    console.log('[check-point] updateCycle end')
    const sql = `UPDATE cycles SET counter = $counter, cycleRecord = $cycleRecord WHERE cycleMarker = $marker `
    await db.run(cycleDatabase, sql, {
      $counter: cycle.counter,
      $cycleRecord: cycle.cycleRecord && SerializeToJsonString(cycle.cycleRecord),
      $marker: marker,
    })
    if (config.VERBOSE) {
      Logger.mainLogger.debug('Updated cycle for counter', cycle.cycleRecord.counter, cycle.cycleMarker)
    }
  } catch (e) {
    Logger.mainLogger.error(e)
    Logger.mainLogger.error('Unable to update Cycle', cycle.cycleMarker)
  }
}

export async function queryCycleByMarker(marker: string): Promise<Cycle> {
  try {
    const sql = `SELECT * FROM cycles WHERE cycleMarker=? LIMIT 1`
    const dbCycle = (await db.get(cycleDatabase, sql, [marker])) as DbCycle
    let cycle: Cycle
    if (dbCycle) {
      cycle = {
        counter: dbCycle.counter,
        cycleRecord: DeSerializeFromJsonString(dbCycle.cycleRecord),
        cycleMarker: dbCycle.cycleMarker,
      }
    }
    if (config.VERBOSE) {
      Logger.mainLogger.debug('cycle marker', cycle)
    }
    return cycle
  } catch (e) {
    Logger.mainLogger.error(e)
    return null
  }
}

export async function queryLatestCycleRecords(count: number): Promise<P2P.CycleCreatorTypes.CycleData[]> {
  if (!Number.isInteger(count)) {
    Logger.mainLogger.error('queryLatestCycleRecords - Invalid count value')
    return []
  }
  try {
    const sql = `SELECT * FROM cycles ORDER BY counter DESC LIMIT ${count ? count : 100}`
    const dbCycles = (await db.all(cycleDatabase, sql)) as DbCycle[]
    const cycleRecords: P2P.CycleCreatorTypes.CycleData[] = []
    if (dbCycles.length > 0) {
      for (const cycle of dbCycles) {
        if (cycle.cycleRecord) cycleRecords.push(DeSerializeFromJsonString(cycle.cycleRecord))
      }
    }
    if (config.VERBOSE) {
      Logger.mainLogger.debug('cycle latest', cycleRecords)
    }
    return cycleRecords
  } catch (e) {
    Logger.mainLogger.error(e)
    return []
  }
}

export async function queryCycleRecordsBetween(
  start: number,
  end: number
): Promise<P2P.CycleCreatorTypes.CycleData[]> {
  try {
    const sql = `SELECT * FROM cycles WHERE counter BETWEEN ? AND ? ORDER BY counter ASC`
    const dbCycles = (await db.all(cycleDatabase, sql, [start, end])) as DbCycle[]
    const cycleRecords: P2P.CycleCreatorTypes.CycleData[] = []
    if (dbCycles.length > 0) {
      for (const cycle of dbCycles) {
        if (cycle.cycleRecord) cycleRecords.push(DeSerializeFromJsonString(cycle.cycleRecord))
      }
    }
    if (config.VERBOSE) {
      Logger.mainLogger.debug('cycle between', cycleRecords)
    }
    return cycleRecords
  } catch (e) {
    Logger.mainLogger.error(e)
    return []
  }
}

export async function queryCyleCount(): Promise<number> {
  let cycles
  try {
    const sql = `SELECT COUNT(*) FROM cycles`
    cycles = await db.get(cycleDatabase, sql, [])
  } catch (e) {
    Logger.mainLogger.error(e)
  }
  if (config.VERBOSE) {
    Logger.mainLogger.debug('Cycle count', cycles)
  }
  if (cycles) cycles = cycles['COUNT(*)']
  else cycles = 0
  return cycles
}

export async function queryCycleByBucketId(bucketId: string): Promise<Cycle[]> {
  try {
    console.log('[check-point] queryCycleByBucketId start')
    // Query cycles where first two chars of hashed counter match bucketId
    const sql = `
      SELECT * FROM cycles 
      WHERE substr(lower(hex(hash(counter))), 1, 2) = ?
      ORDER BY counter ASC
    `
    const dbCycles = (await db.all(cycleDatabase, sql, [bucketId])) as DbCycle[]
    console.log('[check-point] queryCycleByBucketId end', dbCycles) 
    return dbCycles.map(dbCycle => ({
      counter: dbCycle.counter,
      cycleRecord: DeSerializeFromJsonString(dbCycle.cycleRecord),
      cycleMarker: dbCycle.cycleMarker,
    }))
  } catch (e) {
    console.error('[check-point] queryCycleByBucketId error', e)
    Logger.mainLogger.error('[queryCycleByBucketId]', e)
    return []
  }
}
