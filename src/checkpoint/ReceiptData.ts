import { CheckpointBucketManager, CheckpointData, CheckpointType } from './CheckpointData'
import { Receipt as ReceiptType, ArchiverReceipt, SignedReceipt } from '../dbstore/receipts'
import * as ReceiptDB from '../dbstore/receipts'
import * as Logger from '../Logger'
import * as crypto from 'crypto'
import { verifyReceiptData } from '../Data/Collector'
import { verifyAppReceiptData } from '../shardeum/verifyAppReceiptData'
import { safeStringify } from '@shardeum-foundation/lib-types/build/src/utils/functions/stringify'
import * as db from '../dbstore/sqlite3storage'
import { checkpointDatabase } from '../dbstore'

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
  private static instance: ReceiptCheckpointManager;

  private constructor() {
    super({
      validateData: ReceiptCheckpointManager.validateData,
      updateData: ReceiptCheckpointManager.updateData,
    }, CheckpointType.Receipt)
  }

  public static getInstance(): ReceiptCheckpointManager {
    if (!ReceiptCheckpointManager.instance) {
      ReceiptCheckpointManager.instance = new ReceiptCheckpointManager();
    }
    return ReceiptCheckpointManager.instance;
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

  private static async updateData(data: CheckpointData<ReceiptType | ArchiverReceipt>): Promise<void> {
    const { tx, signedReceipt, globalModification } = data.d
    const sortedVoteOffsets = globalModification ? [] : (signedReceipt as SignedReceipt).voteOffsets.sort()
    const medianOffset = sortedVoteOffsets[Math.floor(sortedVoteOffsets.length / 2)] ?? 0
    const applyTimestamp = tx.timestamp + medianOffset * 1000

    const sql = `
      INSERT OR REPLACE INTO checkpoint_data (
        address, timestamp, hash, class_type, bucket_id, data_json, processed, last_update
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `
    const values = [
      data.a,
      data.t,
      data.h,
      2,
      data.d.cycle,
      safeStringify({
        ...data.d,
        receiptId: tx.txId,
        timestamp: tx.timestamp,
        applyTimestamp,
      }),
      false,
      Math.floor(Date.now() / 1000),
    ]

    await db.run(checkpointDatabase, sql, values)
  }
}

// Export the singleton instance
export const receiptCheckpointManager = ReceiptCheckpointManager.getInstance();
