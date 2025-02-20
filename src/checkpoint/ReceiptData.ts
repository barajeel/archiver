import {
  CheckpointBucketManager,
  CheckpointData,
  CheckpointRadixEntry,
  CheckpointRadixDigest,
  CheckpointType,
  DataPersistenceCallbacks,
  CheckpointBucket,
  RadixDigestTally,
} from './CheckpointData'
import { Receipt as ReceiptType, ArchiverReceipt, SignedReceipt } from '../dbstore/receipts'
import * as Logger from '../Logger'
import * as crypto from 'crypto'
import { verifyAppReceiptData } from '../shardeum/verifyAppReceiptData'
import { safeStringify } from '@shardeum-foundation/lib-types/build/src/utils/functions/stringify'
import * as db from '../dbstore/sqlite3storage'
import { receiptDatabase } from '../dbstore'
import { SerializeToJsonString } from '../utils/serialization'

export class ReceiptCheckpointData extends CheckpointData<ReceiptType> {
  constructor(receipt: ReceiptType) {
    const receiptHash = crypto.createHash('sha256').update(safeStringify(receipt)).digest('hex').toLowerCase()

    super(
      receiptHash.substring(0, 2), // address (first 2 chars)
      receipt.tx.timestamp, // timestamp
      receiptHash, // hash
      2, // class type 2 for receipts
      receipt // data
    )
  }
}

export function calculateBucketID(receipt: ReceiptType): string {
  if (!receipt || receipt.tx.timestamp === undefined) {
    Logger.mainLogger.error('Invalid receipt data')
    throw new Error('Invalid receipt data')
  }
  return receipt.cycle.toString()
}

//Represents a single radix entry in a bucket
export class ReceiptCheckpointRadixEntry extends CheckpointRadixEntry<ReceiptType> {
  constructor(radix: string) {
    super(radix)
  }
}

//Represents a single radix entry in a bucket
export class ReceiptCheckpointRadixDigest extends CheckpointRadixDigest {
  constructor(radix: string, hash: string, itemCount: number) {
    super(radix, hash, itemCount)
  }
}

//Represents a single bucket in the system
export class ReceiptCheckpointBucket extends CheckpointBucket<ReceiptType> {
  constructor(
    startTime: number,
    endTime: number,
    bucketID: string,
    validateData: (data: CheckpointData<ReceiptType>) => Promise<boolean>,
    updateData: (data: CheckpointData<ReceiptType>) => Promise<void>
  ) {
    super(startTime, endTime, bucketID, validateData, updateData, CheckpointType.Receipt)
  }

  async update(currentTime: number): Promise<void> {
    // Call parent update first
    super.update(currentTime)
  }
}

class ReceiptCheckpointManager extends CheckpointBucketManager<ReceiptType> {
  private static instance: ReceiptCheckpointManager

  private constructor() {
    const persistenceCallbacks: DataPersistenceCallbacks<ReceiptType | ArchiverReceipt> = {
      validateData: validateData,
      updateData: updateData,
    }
    super(persistenceCallbacks, CheckpointType.Receipt)
  }

  public static getInstance(): ReceiptCheckpointManager {
    if (!ReceiptCheckpointManager.instance) {
      ReceiptCheckpointManager.instance = new ReceiptCheckpointManager()
    }
    return ReceiptCheckpointManager.instance
  }
}

//Represents a tally of all radix entries in the system
export class ReceiptRadixDigestTally extends RadixDigestTally {
  constructor(radix: string) {
    super(radix)
  }
}

async function validateData(data: CheckpointData<ReceiptType>): Promise<boolean> {
  try {
    const verifyHash = crypto.createHash('sha256').update(safeStringify(data.d)).digest('hex').toLowerCase()

    if (verifyHash !== data.h) {
      Logger.mainLogger.error('[ReceiptCheckpoint] validateData failed:', verifyHash, data.h)
      return false
    }

    const appValidation = await verifyAppReceiptData(data.d, null, [], [])
    return appValidation.valid
  } catch (err) {
    Logger.mainLogger.error('[ReceiptCheckpoint] validateData failed:', err)
    return false
  }
}

async function updateData(data: CheckpointData<ReceiptType>): Promise<void> {
  try {
    // Insert/Update into checkpoint_data table
    const columns = [
      'receiptId',
      'tx',
      'cycle',
      'applyTimestamp',
      'timestamp',
      'signedReceipt',
      'afterStates',
      'beforeStates',
      'appReceiptData',
      'executionShardKey',
      'globalModification',
    ]
    const receipt = data.d
    const sql = `INSERT OR REPLACE INTO receipts (${columns.join(', ')}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

    // Calculate median offset for apply timestamp
    const sortedVoteOffsets = receipt.globalModification
      ? []
      : (receipt.signedReceipt as SignedReceipt).voteOffsets.sort()
    const medianOffset = sortedVoteOffsets[Math.floor(sortedVoteOffsets.length / 2)] ?? 0
    const applyTimestamp = receipt.tx.timestamp + medianOffset * 1000

    // Map the `receipt` object to match the columns
    // const values = [
    //   receipt.receiptId,
    //   typeof receipt.tx === 'object' ? SerializeToJsonString(receipt.tx) : receipt.tx,
    //   receipt.cycle,
    //   applyTimestamp,
    //   receipt.timestamp,
    //   typeof receipt.signedReceipt === 'object'
    //     ? SerializeToJsonString(receipt.signedReceipt)
    //     : receipt.signedReceipt,
    //   typeof receipt.afterStates === 'object'
    //     ? SerializeToJsonString(receipt.afterStates)
    //     : receipt.afterStates,
    //   typeof receipt.beforeStates === 'object'
    //     ? SerializeToJsonString(receipt.beforeStates)
    //     : receipt.beforeStates,
    //   typeof receipt.appReceiptData === 'object'
    //     ? SerializeToJsonString(receipt.appReceiptData)
    //     : receipt.appReceiptData,
    //   receipt.executionShardKey,
    //   receipt.globalModification,
    // ]
    const values = columns.map((column) =>
      typeof receipt[column] === 'object'
        ? SerializeToJsonString(receipt[column]) // Serialize objects to JSON strings
        : receipt[column]
    )

    console.log('writing receipt', receipt)
    console.log('sql', sql)
    console.log('values', values)
    // Execute the query directly (single-row insert)
    await db.run(receiptDatabase, sql, values)

    Logger.mainLogger.debug('receipt checkpoint data stored')
  } catch (err) {
    Logger.mainLogger.error('Failed to store receipt checkpoint data:', err)
    throw err
  }
}

// Export the singleton instance
export const receiptCheckpointManager = ReceiptCheckpointManager.getInstance()
