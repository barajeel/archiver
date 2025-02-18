import { CheckpointBucketManager, CheckpointData, CheckpointType } from './CheckpointData'
import { Receipt as ReceiptType, ArchiverReceipt, SignedReceipt } from '../dbstore/receipts'
import * as ReceiptDB from '../dbstore/receipts'
import * as Logger from '../Logger'
import * as crypto from 'crypto'
import { verifyReceiptData } from '../Data/Collector'
import { verifyAppReceiptData } from '../shardeum/verifyAppReceiptData'
import { safeStringify } from '@shardeum-foundation/lib-types/build/src/utils/functions/stringify'
import * as db from '../dbstore/sqlite3storage'
import { checkpointDatabase, receiptDatabase } from '../dbstore'
import { SerializeToJsonString } from '../utils/serialization'

export class ReceiptCheckpointData extends CheckpointData<ReceiptType | ArchiverReceipt> {
  constructor(receipt: ReceiptType | ArchiverReceipt) {
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

class ReceiptCheckpointManager extends CheckpointBucketManager<ReceiptType | ArchiverReceipt> {
  private static instance: ReceiptCheckpointManager

  private constructor() {
    super(
      {
        validateData: ReceiptCheckpointManager.validateData,
        updateData: ReceiptCheckpointManager.updateData,
      },
      CheckpointType.Receipt
    )
  }

  public static getInstance(): ReceiptCheckpointManager {
    if (!ReceiptCheckpointManager.instance) {
      ReceiptCheckpointManager.instance = new ReceiptCheckpointManager()
    }
    return ReceiptCheckpointManager.instance
  }

  // public addReceipt(receipt: ReceiptType | ArchiverReceipt): void {
  //   const checkpointData = new ReceiptCheckpointData(receipt)
  //   this.addData(checkpointData, receipt.cycle.toString())
  // } // TODO : the manager should handle this outside instead of being handled internally, checkout cycleCheckpointManager.addData in dbStore

  private static async validateData(data: CheckpointData<ReceiptType | ArchiverReceipt>): Promise<boolean> {
    try {
      const verifyHash = crypto.createHash('sha256').update(safeStringify(data.d)).digest('hex').toLowerCase()

      if (verifyHash !== data.h) return false

      // const validationResult = await verifyReceiptData(data.d, true)
      // if (!validationResult.success) return false

      const appValidation = await verifyAppReceiptData(data.d, null, [], [])
      return appValidation.valid
    } catch (err) {
      Logger.mainLogger.error('[ReceiptCheckpoint] validateData failed:', err)
      return false
    }
  }

  private static async updateData(data: CheckpointData<ReceiptType>): Promise<void> {
    try {
      // Insert/Update into checkpoint_data table
      console.log('[check-point] updateData', data)

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
      console.log('[my-log] receipt insert sql: ', sql)

      // Calculate median offset for apply timestamp
      const sortedVoteOffsets = receipt.globalModification
        ? []
        : (receipt.signedReceipt as SignedReceipt).voteOffsets.sort()
      const medianOffset = sortedVoteOffsets[Math.floor(sortedVoteOffsets.length / 2)] ?? 0
      const applyTimestamp = receipt.tx.timestamp + medianOffset * 1000

      // Map the `receipt` object to match the columns
      const values = [
        receipt.receiptId,
        typeof receipt.tx === 'object' ? SerializeToJsonString(receipt.tx) : receipt.tx,
        receipt.cycle,
        applyTimestamp,
        receipt.timestamp,
        typeof receipt.signedReceipt === 'object'
          ? SerializeToJsonString(receipt.signedReceipt)
          : receipt.signedReceipt,
        typeof receipt.afterStates === 'object'
          ? SerializeToJsonString(receipt.afterStates)
          : receipt.afterStates,
        typeof receipt.beforeStates === 'object'
          ? SerializeToJsonString(receipt.beforeStates)
          : receipt.beforeStates,
        typeof receipt.appReceiptData === 'object'
          ? SerializeToJsonString(receipt.appReceiptData)
          : receipt.appReceiptData,
        receipt.executionShardKey,
        receipt.globalModification,
      ]

      // Execute the query directly (single-row insert)
      await db.run(receiptDatabase, sql, values)

      console.log('[check-point] updateData stored checkpoint data', data.h)
      Logger.mainLogger.debug('[CheckpointData] Stored checkpoint data:', data.h)
    } catch (err) {
      console.error('[check-point] updateData Failed to store checkpoint data:', err)
      Logger.mainLogger.error('[CheckpointData] Failed to store checkpoint data:', err)
      throw err
    }
  }
}

// Export the singleton instance
export const receiptCheckpointManager = ReceiptCheckpointManager.getInstance()
