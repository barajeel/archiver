import * as db from '../dbstore/sqlite3storage'
import {
  CheckpointBucketManager,
  CheckpointData,
  CheckpointRadixEntry,
  CheckpointRadixDigest,
  CheckpointBucket,
  CheckpointType,
} from './CheckpointData'
import { config } from '../Config'
import * as crypto from 'crypto'
import { OriginalTxData } from '../dbstore/originalTxsData'
import { safeStringify } from '@shardeum-foundation/lib-types/build/src/utils/functions/stringify'
import { checkpointDatabase, originalTxDataDatabase } from '../dbstore'
import * as Logger from '../Logger'

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
    console.log('[check-point] updateData originalTx', data)
    const values = [
      data.d.txId, // txId
      data.t, // timestamp
      data.d.cycle, // cycle
      safeStringify(data.d.originalTxData), // originalTxData
    ]

    const columns = ['txId', 'timestamp', 'cycle', 'originalTxData']

    const placeholders = columns.map(() => '?').join(', ')
    const sql = `INSERT OR REPLACE INTO originalTxsData (${columns.join(', ')}) VALUES (${placeholders})`

    await db.run(originalTxDataDatabase, sql, values)
    console.log('[check-point] updateData stored originalTx checkpoint data', data.h)
    Logger.mainLogger.debug('[CheckpointData] Stored originalTx checkpoint data:', data.h)
  } catch (err) {
    console.error('[check-point] updateData Failed to store originalTx checkpoint data:', err)
    Logger.mainLogger.error('[CheckpointData] Failed to store originalTx checkpoint data:', err)
    throw err
  }
}

// Define the validateData function
async function validateData(data: CheckpointData<OriginalTxData>): Promise<boolean> {
  // Reuse existing validation logic
  const { validateOriginalTxData } = require('../Data/Collector')
  return validateOriginalTxData(data.d)
}

// Persist checkpoint data to main table
async function persistToMainTable(bucketId: string): Promise<void> {
  try {
    console.log('[check-point] persistToMainTable originalTx start', bucketId)
    const sql = `
      SELECT * FROM checkpoint_data 
      WHERE bucket_id = ? AND class_type = 1 AND processed = false
    `
    const checkpoints: any[] = await db.all(checkpointDatabase, sql, [bucketId])
    console.log('[check-point] persistToMainTable originalTx found', checkpoints.length)

    // Update originalTxsData table and mark as processed
    for (const checkpoint of checkpoints) {
      const originalTxData = JSON.parse(checkpoint.data_json)
      await require('../dbstore/originalTxsData').bulkInsertOriginalTxsData([originalTxData])

      await db.run(checkpointDatabase, 'UPDATE checkpoint_data SET processed = true WHERE hash = ?', [
        checkpoint.hash,
      ])
    }
    console.log('[check-point] persistToMainTable originalTx end')
  } catch (err) {
    console.error('[check-point] persistToMainTable Failed to persist originalTx bucket:', bucketId, err)
    Logger.mainLogger.error('[CheckpointData] Failed to persist originalTx bucket:', bucketId, err)
    throw err
  }
}
