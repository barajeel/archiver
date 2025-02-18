import * as db from '../dbstore/sqlite3storage'
import {
  CheckpointBucketManager,
  CheckpointData,
  CheckpointRadixEntry,
  CheckpointRadixDigest,
  CheckpointBucket,
  CheckpointType,
} from './CheckpointData'
import * as crypto from 'crypto'
import { OriginalTxData } from '../dbstore/originalTxsData'
import { safeStringify } from '@shardeum-foundation/lib-types/build/src/utils/functions/stringify'
import { originalTxDataDatabase } from '../dbstore'
import * as Logger from '../Logger'
import { SerializeToJsonString } from '../utils/serialization'

export class OriginalTxCheckpointData extends CheckpointData<OriginalTxData> {
  constructor(data: OriginalTxData) {
    const originalTxHash = crypto.createHash('sha256').update(safeStringify(data)).digest('hex').toLowerCase()

    super(
      originalTxHash.substring(0, 2), // address (first 2 chars)
      data.timestamp, // timestamp
      originalTxHash, // hash
      1, // class_type 1 for originalTx
      data // data
    )
  }
}

export class OriginalTxCheckpointRadixEntry extends CheckpointRadixEntry<OriginalTxData> {
  constructor(radix: string) {
    super(radix)
  }
}

export class OriginalTxCheckpointRadixDigest extends CheckpointRadixDigest {
  constructor(radix: string, hash: string, itemCount: number) {
    super(radix, hash, itemCount)
  }
}

export class OriginalTxCheckpointBucket extends CheckpointBucket<OriginalTxData> {
  constructor(
    startTime: number,
    endTime: number,
    bucketID: string,
    validateData: (data: CheckpointData<OriginalTxData>) => Promise<boolean>,
    updateData: (data: CheckpointData<OriginalTxData>) => Promise<void>
  ) {
    super(startTime, endTime, bucketID, validateData, updateData, CheckpointType.OriginalTx)
  }
}

class OriginalTxCheckpointManager extends CheckpointBucketManager<OriginalTxData> {
  private static instance: OriginalTxCheckpointManager

  private constructor() {
    super(
      {
        validateData,
        updateData,
      },
      CheckpointType.OriginalTx
    )
  }

  public static getInstance(): OriginalTxCheckpointManager {
    if (!OriginalTxCheckpointManager.instance) {
      OriginalTxCheckpointManager.instance = new OriginalTxCheckpointManager()
    }
    return OriginalTxCheckpointManager.instance
  }
}

// Export the singleton instance
export const originalTxCheckpointManager = OriginalTxCheckpointManager.getInstance()

// Define the updateData function
async function updateData(data: CheckpointData<OriginalTxData>): Promise<void> {
  try {
    // Insert/Update into originalTxsData table
    const columns = ['txId', 'timestamp', 'cycle', 'originalTxData']
    const originalTx = data.d
    const sql = `INSERT OR REPLACE INTO originalTxsData (${columns.join(', ')}) VALUES (?, ?, ?, ?)`

    // Map the `originalTx` object to match the columns
    const values = [
      originalTx.txId,
      data.t,
      originalTx.cycle,
      typeof originalTx.originalTxData === 'object'
        ? SerializeToJsonString(originalTx.originalTxData) // Serialize objects to JSON
        : originalTx.originalTxData,
    ]

    // Execute the query directly (single-row insert)
    await db.run(originalTxDataDatabase, sql, values)

    Logger.mainLogger.debug('originalTx checkpoint data stored')
  } catch (err) {
    Logger.mainLogger.error('Failed to store originalTx checkpoint data:', err)
    throw err
  }
}

// Define the validateData function
async function validateData(data: CheckpointData<OriginalTxData>): Promise<boolean> {
  // Reuse existing validation logic
  const { validateOriginalTxData } = require('../Data/Collector')
  return validateOriginalTxData(data.d)
}
