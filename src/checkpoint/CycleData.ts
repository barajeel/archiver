import { Cycle } from '../dbstore/types'
import {
  CheckpointBucket,
  CheckpointBucketManager,
  CheckpointData,
  CheckpointRadixDigest,
  CheckpointRadixEntry,
  RadixDigestTally,
  DataPersistenceCallbacks,
  CheckpointType,
} from './CheckpointData'
import * as crypto from 'crypto'
import { safeStringify } from '@shardeum-foundation/lib-types/build/src/utils/functions/stringify'
import { queryCycleByMarker, updateCycle } from '../dbstore/cycles'
import * as Logger from '../Logger'
import * as db from '../dbstore/sqlite3storage'
import { queryFromArchivers } from '../API'
import { RequestDataType } from '../API'
import { SerializeToJsonString } from '../utils/serialization'
import { cycleDatabase } from '../dbstore'

//Represents a single piece of cycle data``
export class CycleCheckpointData extends CheckpointData<Cycle> {
  constructor(cycle: Cycle) {
    const cycleHash = crypto.createHash('sha256').update(safeStringify(cycle)).digest('hex').toLowerCase()

    super(
      cycleHash.substring(0, 2), // address (first 2 chars)
      cycle.cycleRecord.start, // timestamp from cycleRecord
      cycleHash, // hash
      0, // class type 0 for cycle
      cycle // data
    )
  }
}

export function calculateBucketID(cycle: Cycle): string {
  console.log('[check-point] calculateBucketID', cycle)
  if (!cycle || !cycle.counter) {
    console.error('[check-point] calculateBucketID Invalid cycle data')
    Logger.mainLogger.error('[calculateBucketID] Invalid cycle data')
    throw new Error('Invalid cycle data')
  }

  return cycle.counter.toString()
}

//Represents a single radix entry in a bucket
export class CycleCheckpointRadixEntry extends CheckpointRadixEntry<Cycle> {
  constructor(radix: string) {
    super(radix)
  }
}

//Represents a single radix entry in a bucket
export class CycleCheckpointRadixDigest extends CheckpointRadixDigest {
  constructor(radix: string, hash: string, itemCount: number) {
    super(radix, hash, itemCount)
  }
}

//Represents a single bucket in the system
export class CycleCheckpointBucket extends CheckpointBucket<Cycle> {
  constructor(
    startTime: number,
    endTime: number,
    bucketID: string,
    validateData: (data: CheckpointData<Cycle>) => Promise<boolean>,
    updateData: (data: CheckpointData<Cycle>) => Promise<void>
  ) {
    super(startTime, endTime, bucketID, validateData, updateData, CheckpointType.Cycle)
  }

  async update(currentTime: number): Promise<void> {
    const bucketAge = currentTime - this.startTime

    // Call parent update first
    super.update(currentTime)

    // Only persist if bucket has reached give up age
    // if (bucketAge > config.checkpointBucketConfig.GiveUpAge) {
    //   console.log('[check-point] CycleCheckpointBucket update persistToMainTable', this.bucketID)
    //   await persistToMainTable(this.bucketID)
    // }
  }
}

//Manages all buckets, routes incoming data to the correct bucket, and does periodic updates
class CycleCheckpointManager extends CheckpointBucketManager<Cycle> {
  private static instance: CycleCheckpointManager

  private constructor() {
    const persistenceCallbacks: DataPersistenceCallbacks<Cycle> = {
      validateData,
      updateData,
    }
    super(persistenceCallbacks, CheckpointType.Cycle)
  }

  public static getInstance(): CycleCheckpointManager {
    if (!CycleCheckpointManager.instance) {
      CycleCheckpointManager.instance = new CycleCheckpointManager()
    }
    return CycleCheckpointManager.instance
  }
}

// Export the singleton instance
export const cycleCheckpointManager = CycleCheckpointManager.getInstance()

//Represents a tally of all radix entries in the system
export class CycleRadixDigestTally extends RadixDigestTally {
  constructor(radix: string) {
    super(radix)
  }
}

// Define the validateData function
async function validateData(data: CheckpointData<Cycle>): Promise<boolean> {
  const cycle = data.d
  console.log('[check-point] validateData', data)
  // Basic validation checks
  if (!cycle || !cycle.counter || !cycle.cycleMarker || !cycle.cycleRecord) {
    console.error('[check-point] validateData Missing required cycle fields')
    Logger.mainLogger.error('[CycleValidation] Missing required cycle fields')
    return false
  }

  // Validate cycle record fields
  if (!cycle.cycleRecord.start || !cycle.cycleRecord.counter) {
    console.error('[check-point] validateData Invalid cycle record fields')
    Logger.mainLogger.error('[CycleValidation] Invalid cycle record fields')
    return false
  }

  // Verify timestamp matches cycle record start time
  if (data.t !== cycle.cycleRecord.start) {
    console.error('[check-point] validateData Timestamp mismatch with cycle record')
    Logger.mainLogger.error('[CycleValidation] Timestamp mismatch with cycle record')
    return false
  }

  // Verify address matches hash of cycle counter
  const expectedAddress = crypto
    .createHash('sha256')
    .update(safeStringify(cycle))
    .digest('hex')
    .toLowerCase()
    .substring(0, 2)

  if (data.a !== expectedAddress) {
    console.error('[check-point] validateData Address mismatch')
    Logger.mainLogger.error('[CycleValidation] Address mismatch')
    return false
  }

  // Verify hash matches data
  const calculatedHash = crypto.createHash('sha256').update(safeStringify(cycle)).digest('hex').toLowerCase()

  if (calculatedHash !== data.h) {
    console.error('[check-point] validateData Hash mismatch')
    Logger.mainLogger.error('[CycleValidation] Hash mismatch')
    return false
  }

  // Verify cycle exists in database
  // try {
  //   const existingCycle = await queryCycleByMarker(cycle.cycleMarker)
  //   if (!existingCycle) {
  //     console.error('[check-point] validateData Cycle not found in database')
  //     Logger.mainLogger.error('[CycleValidation] Cycle not found in database')
  //     return false
  //   }
  // } catch (err) {
  //   console.error('[check-point] validateData Database query failed:', err)
  //   Logger.mainLogger.error('[CycleValidation] Database query failed:', err)
  //   return false
  // }

  return true
}

// Define the updateData function
async function updateData(data: CheckpointData<Cycle>): Promise<void> {
  try {
    // Insert/Update into checkpoint_data table
    console.log('[check-point] updateData', data)

    const columns = ['cycleMarker', 'counter', 'cycleRecord']
    const cycle = data.d
    const sql = `INSERT OR REPLACE INTO cycles (${columns.join(', ')}) VALUES (?, ?, ?)`
    console.log('[my-log] cycle insert sql: ', sql)

    // Map the `cycle` object to match the columns
    const values = [
      cycle.cycleMarker,
      cycle.counter,
      typeof cycle.cycleRecord === 'object'
        ? SerializeToJsonString(cycle.cycleRecord) // Serialize objects to JSON
        : cycle.cycleRecord,
    ]

    // Execute the query directly (single-row insert)
    await db.run(cycleDatabase, sql, values)

    console.log('[check-point] updateData stored checkpoint data', data.h)
    Logger.mainLogger.debug('[CheckpointData] Stored checkpoint data:', data.h)
  } catch (err) {
    console.error('[check-point] updateData Failed to store checkpoint data:', err)
    Logger.mainLogger.error('[CheckpointData] Failed to store checkpoint data:', err)
    throw err
  }
}

async function persistToMainTable(bucketId: string): Promise<void> {
  try {
    // Get all checkpoint data for this bucket
    console.log('[check-point] persistToMainTable start', bucketId)
    const sql = `
      SELECT * FROM checkpoint_data 
      WHERE bucket_id = ? AND processed = false
    `
    // const checkpoints: any[] = await db.all(checkpointDatabase, sql, [bucketId])
    // console.log('[check-point] persistToMainTable end', checkpoints)

    // Update cycles table and mark as processed
    // for (const checkpoint of checkpoints) {
    //   const cycle = JSON.parse(checkpoint.data_json)
    //   await updateCycle(cycle.cycleMarker, cycle)

    //   await db.run(checkpointDatabase, 'UPDATE checkpoint_data SET processed = true WHERE hash = ?', [
    //     checkpoint.hash,
    //   ])
    // }
    console.log('[check-point] persistToMainTable end')
  } catch (err) {
    console.error('[check-point] persistToMainTable Failed to persist bucket:', bucketId, err)
    Logger.mainLogger.error('[CheckpointData] Failed to persist bucket:', bucketId, err)
    throw err
  }
}

// Create a singleton instance
// export const cycleCheckpointManager = new CycleCheckpointManager()
export async function getCheckpointDataFromArchiver(): Promise<any[]> {
  try {
    console.log('[check-point] getCheckpointDataFromArchiver start')
    const response = await queryFromArchivers(RequestDataType.CHECKPOINT, {}, 60 * 1000)
    return response as any[]
  } catch (err) {
    console.error('[check-point] getCheckpointDataFromArchiver Failed to get checkpoint data:', err)
    Logger.mainLogger.error('[Data] Failed to get checkpoint data:', err)
    throw err
  }
}
