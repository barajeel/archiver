import * as fs from 'fs'
import * as Logger from '../Logger'
import { otherArchivers } from '../State'
import { postJson } from '../P2P'
import { config } from '../Config'
import * as crypto from 'crypto'
import { safeStringify } from '@shardeum-foundation/lib-types/build/src/utils/functions/stringify'

export enum CheckpointType {
  Cycle = 0,
  OriginalTx = 1,
  Receipt = 2,
}

// Represents a single piece of data in the system.
export class CheckpointData<T> {
  // Address used to determine the radix entry
  a: string
  // Timestamp of the data
  t: number
  // Unique hash/identifier for the data
  h: string
  // Class/type identifier
  c: number
  // Actual data payload
  d: T

  constructor(a: string, t: number, h: string, c: number, d: T) {
    this.a = a
    this.t = t
    this.h = h
    this.c = c
    this.d = d
  }
}

// Summarizes the data in one radix slot.
export class CheckpointRadixDigest {
  radix: string
  hash: string
  itemCount: number

  constructor(radix: string, hash: string, itemCount: number) {
    this.radix = radix
    this.hash = hash
    this.itemCount = itemCount
  }
}

// Represents a single radix entry in a bucket.
export class CheckpointRadixEntry<T> {
  digest: CheckpointRadixDigest
  sortedData: CheckpointData<T>[]

  constructor(radix: string) {
    this.digest = new CheckpointRadixDigest(radix, '', 0)
    this.sortedData = []
  }

  updateDigest(): void {
    // Sort data by address if not already sorted
    this.sortedData.sort((left: CheckpointData<T>, right: CheckpointData<T>) => {
      // Sort primarily by address
      if (left.a < right.a) return -1
      if (left.a > right.a) return 1
      // Then by class type
      if (left.c < right.c) return -1
      if (left.c > right.c) return 1
      return 0
    })

    // Update the digest based on the sorted data
    this.digest.hash = this.computeHash()
    this.digest.itemCount = this.sortedData.length
  }

  private computeHash(): string {
    const hashToCompute = this.sortedData
      .map((d) => d.h) // Use the unique hash/identifier
      .join('') // Join all hashes together

    const hash = crypto.createHash('sha256').update(safeStringify(hashToCompute)).digest('hex').toLowerCase()
    return hash
  }
}

export interface DataPersistenceCallbacks<T> {
  updateData: (data: CheckpointData<T>) => Promise<void>
  validateData: (data: CheckpointData<T>) => Promise<boolean>
}

// Manages all buckets, routes incoming data to the correct bucket, and does periodic updates.
export class CheckpointBucketManager<T> {
  checkpointBuckets: Map<string, CheckpointBucket<T>>
  validateData: (data: CheckpointData<T>) => Promise<boolean>
  updateData: (data: CheckpointData<T>) => Promise<void>
  checkpointType: CheckpointType

  constructor(
    private persistenceCallbacks: DataPersistenceCallbacks<T>,
    checkpointType: CheckpointType
  ) {
    this.checkpointBuckets = new Map<string, CheckpointBucket<T>>()
    this.validateData = persistenceCallbacks.validateData
    this.updateData = persistenceCallbacks.updateData
    this.checkpointType = checkpointType
  }

  addData(data: CheckpointData<T>, bucketID: string): void {
    let bucket = this.checkpointBuckets.get(bucketID)
    if (!bucket) {
      Logger.mainLogger.debug(`bucketID ${bucketID} not found, creating new bucket`)
      const startTime = Math.floor(data.t)
      const endTime = startTime + 60 // for 1 minute buckets
      bucket = new CheckpointBucket<T>(
        startTime,
        endTime,
        bucketID,
        this.validateData,
        this.updateData,
        this.checkpointType
      )
      this.checkpointBuckets.set(bucketID, bucket) // adding an entry that maps the CheckpointType object to its contents, the key here is the address
    }
    Logger.mainLogger.debug(`Adding data to bucket ${bucketID}`)
    bucket.addData(data)
  }

  async update(): Promise<void> {
    try {
      // update the naming convention here to indicate time change and not data updation
      Logger.mainLogger.debug('Updating checkpoint buckets')
      const currentTime = Math.floor(Date.now() / 1000)
      const toRemove: string[] = []
      for (const [id, bucket] of this.checkpointBuckets.entries()) {
        if (!bucket) {
          continue
        }
        if (currentTime > bucket.GiveUpAge) {
          // We consider this bucket "failed" or "too old" => persist & alert
          Logger.mainLogger.debug(`Bucket ${bucket.bucketID} exceeded GiveUpAge. Persisting & removing.`)
          if (bucket.hasUpdatesToShare) {
            Logger.mainLogger.debug(
              `Bucket ${bucket.bucketID} has updates to share. Writing to file and alerting.`
            )
            bucket.writeToFileAndAlert()
          } else {
            Logger.mainLogger.debug(
              `Bucket ${bucket.bucketID} is older than giveUpAge and has no updates to share`
            )

            // Persist data to local storage
            const promises: Promise<void>[] = []
            for (const entry of bucket.radixEntries.values()) {
              for (const dataItem of entry.sortedData) {
                promises.push(bucket.updateData(dataItem))
              }
            }
            await Promise.all(promises)
          }
          toRemove.push(id)
        } else {
          // Let the bucket do its normal update
          bucket.update(currentTime)
        }
      }

      // Remove the stale buckets
      for (const id of toRemove) {
        this.checkpointBuckets.delete(id)
      }
    } catch (err) {
      Logger.mainLogger.error('Error in update:', err)
    }
  }

  onHashDigestsReceived(
    senderAddress: string,
    bucketID: string,
    radixDigests: CheckpointRadixDigest[]
  ): void {
    const bucket = this.checkpointBuckets.get(bucketID)
    if (bucket) {
      Logger.mainLogger.debug(`Adding data to bucket ${bucketID}`)
      bucket.onHashDigestsReceived(senderAddress, bucketID, radixDigests)
    }
  }

  onExchangeRadixEntries(bucketID: string, entries: CheckpointRadixEntry<T>[]): CheckpointRadixEntry<T>[] {
    // receives a list of entries which contain radix metadata ( radixDigest ) and the payload for a respective radix ( radix Sorted Data )
    const bucket = this.checkpointBuckets.get(bucketID)
    if (!bucket) {
      Logger.mainLogger.error(`no bucket found for ID=${bucketID}`)
      return []
    }

    bucket.onExchangeRadixEntries(bucketID, entries)

    const result: CheckpointRadixEntry<T>[] = []
    for (const incomingEntry of entries) {
      const localEntry = bucket.radixEntries.get(incomingEntry.digest.radix)
      if (!localEntry) {
        continue
      }
      localEntry.updateDigest()
      result.push(localEntry)
    }

    return result
  }
}

// Represents a single bucket in the system.
export class CheckpointBucket<T> {
  startTime: number
  endTime: number
  bucketID: string
  hasUpdatesToShare: boolean
  sentDigestsCount: number
  receivedDigestCount: number
  lastProcessedDigestCount: number
  radixEntries: Map<string, CheckpointRadixEntry<T>>
  peerRadixDigests: Map<string, RadixDigestTally>
  validateData: (data: CheckpointData<T>) => Promise<boolean>
  updateData: (data: CheckpointData<T>) => Promise<void>
  GiveUpAge: number
  BucketMatureAge: number
  checkpointType: CheckpointType

  constructor(
    startTime: number,
    endTime: number,
    bucketID: string,
    validateData: (data: CheckpointData<T>) => Promise<boolean>,
    updateData: (data: CheckpointData<T>) => Promise<void>,
    checkpointType: CheckpointType
  ) {
    this.startTime = startTime
    this.endTime = endTime
    this.bucketID = bucketID
    this.hasUpdatesToShare = true
    this.sentDigestsCount = 0
    this.receivedDigestCount = 0
    this.lastProcessedDigestCount = 0
    this.radixEntries = new Map<string, CheckpointRadixEntry<T>>()
    this.peerRadixDigests = new Map<string, RadixDigestTally>()
    this.validateData = validateData
    this.updateData = updateData
    this.GiveUpAge = this.startTime + config.checkpointBucketConfig.GiveUpAge
    this.BucketMatureAge = this.startTime + config.checkpointBucketConfig.BucketMatureAge
    this.checkpointType = checkpointType
    // Initialize all possible 256 radix entries (16x16 for 2 hex chars)
    for (let i = 0; i < 256; i++) {
      // Convert i into a hex string without leading zeros
      const hexStr = i.toString(16)
      this.radixEntries.set(hexStr, new CheckpointRadixEntry<T>(hexStr))
      this.peerRadixDigests.set(hexStr, new RadixDigestTally(hexStr))
    }
  }

  async addData(data: CheckpointData<T>): Promise<void> {
    if (this.validateData) {
      const isValid = await this.validateData(data)
      if (!isValid) {
        Logger.mainLogger.error('Validation failed for data:', data)
        return
      }
    }

    const address = data.a.toLowerCase()
    const radix = address.substring(0, 2)

    let entry = this.radixEntries.get(radix)
    if (!entry) {
      // Should not occur as we have pre initialized radix entries
      Logger.mainLogger.error(`Radix ${radix} not found for bucket ${this.bucketID}`)
      return
    }

    // Add data to memory
    entry.sortedData.push(data)

    // Update digest and mark for sharing
    entry.updateDigest()
    this.hasUpdatesToShare = true
  }

  async shareRadixDigests(radixList?: string[]): Promise<void> {
    // Only share if we have updates pending
    if (!this.hasUpdatesToShare) {
      Logger.mainLogger.debug(`Bucket ${this.bucketID} has no updates to share`)
      return
    }

    const digests: CheckpointRadixDigest[] = []

    // Determine which radixes we need to process
    const radixes = radixList ?? Array.from(this.radixEntries.keys())

    // Update each relevant entry and collect its digest
    for (const radix of radixes) {
      const entry = this.radixEntries.get(radix)
      if (!entry) {
        continue
      }
      entry.updateDigest()
      digests.push(entry.digest)
    }

    // If there are no digests to share, set hasUpdatesToShare to false to avoid redundant attempts
    if (digests.length === 0) {
      Logger.mainLogger.debug(`Bucket ${this.bucketID} has no digests to share for the specified radix list`)
      this.hasUpdatesToShare = false
      return
    }

    // Gather a list of peers from our configuration
    const peers = otherArchivers.map((archiver) => `${archiver.ip}:${archiver.port}`)

    // Send digests to all peers
    const sharePromises = peers.map((peerAddress) =>
      postJson(`http://${peerAddress}/shareCheckpointRadixDigests`, {
        //Add verification to this endpoint
        senderAddress: `${config.ARCHIVER_IP}:${config.ARCHIVER_PORT}`,
        bucketID: this.bucketID,
        radixDigests: safeStringify(digests),
        checkpointType: this.checkpointType,
      }).catch((err) => {
        Logger.mainLogger.error(`Failed to share digests with peer ${peerAddress}:`, err)
      })
    )

    try {
      // Wait for all share attempts to complete (successful or not)
      await Promise.allSettled(sharePromises)
      this.sentDigestsCount++

      // If at least one share attempt happened, we can reset hasUpdatesToShare.
      // If subsequent repairs occur, hasUpdatesToShare will be set back to true elsewhere.
      if (sharePromises.length > 0) {
        this.hasUpdatesToShare = false
      }
    } catch (err) {
      Logger.mainLogger.error('Error in shareRadixDigests:', err)
    }
  }

  update(currentTime: number): void {
    const bucketAge = currentTime - this.startTime

    // Check for give up condition (20 minutes)
    if (bucketAge > config.checkpointBucketConfig.GiveUpAge) {
      Logger.mainLogger.error(`Bucket ${this.bucketID} giving up`)
      this.writeToFileAndAlert()
      return
    }

    // Check if bucket has matured (11 minutes) and has updates to share
    if (bucketAge > config.checkpointBucketConfig.BucketMatureAge && this.hasUpdatesToShare) {
      Logger.mainLogger.debug(`Bucket ${this.bucketID} sharing radix digests`)
      this.shareRadixDigests()
    }

    // Check for consensus updates if we've received new digests since last processing
    // and we've sent at least one digest
    if (this.sentDigestsCount > 0 && this.receivedDigestCount > this.lastProcessedDigestCount) {
      Logger.mainLogger.debug(`Bucket ${this.bucketID} evaluating digest consensus`)
      this.evaluateDigestConsensus()
    }
  }

  public writeToFileAndAlert(): void {
    // TODO : dont see any alerting going on over here.
    try {
      const bucketData = {
        bucketID: this.bucketID,
        startTime: this.startTime,
        endTime: this.endTime,
        radixEntries: Array.from(this.radixEntries.entries()),
        peerDigests: Array.from(this.peerRadixDigests.entries()),
      }

      const filename = `failed-bucket-${this.bucketID}-${this.startTime}.json`
      Logger.mainLogger.debug(`Writing bucket id ${this.bucketID} data to file ${filename}`)
      // Write to file
      fs.writeFileSync(filename, JSON.stringify(bucketData, null, 2))
    } catch (err) {
      Logger.mainLogger.error(`Bucket ${this.bucketID} failed to reach consensus after timeout.`)
    }
  }

  evaluateDigestConsensus(): void {
    // If we have no peer data, nothing to evaluate
    if (this.peerRadixDigests.size === 0) {
      this.lastProcessedDigestCount = this.receivedDigestCount
      Logger.mainLogger.debug(`Bucket ${this.bucketID} has no peer data to evaluate consensus`)
      return
    }

    // totalArchivers counts ourself (+1) and external peers
    const totalArchivers = otherArchivers.length + 1
    // "Majority" means more than half.
    // e.g., if totalArchivers = 5, the threshold is 3
    const majorityThreshold = Math.floor(totalArchivers / 2) + 1

    for (const [radix, tally] of this.peerRadixDigests) {
      const localEntry = this.radixEntries.get(radix)
      if (!localEntry) {
        continue
      }

      // Our local hash for this radix
      const ourHash = localEntry.digest.hash

      // Start with our own "vote" of 1, then add any matching votes from peers
      const ourPeerVotes = tally.hashTally.get(ourHash) || 0
      const ourHashCount = 1 + ourPeerVotes

      // If our hash count is below the majority threshold, we need to try repairs
      if (ourHashCount < majorityThreshold) {
        // Gather all peers who currently have a different hash
        const differingPeers = Array.from(tally.peerDigests.entries())
          .filter(([_, digest]) => digest.hash !== ourHash)
          .map(([peer]) => peer)

        // Attempt a repair with one random peer, if any
        if (differingPeers.length > 0) {
          const randomPeer = differingPeers[Math.floor(Math.random() * differingPeers.length)]
          // Attempt repair
          this.exchangeAndRepairRadix(randomPeer, radix).catch((err) => {
            Logger.mainLogger.error(`Failed to repair radix ${radix} with peer ${randomPeer}:`, err)
          })
        }
      }
    }

    // After evaluating all radixes, update the lastProcessedDigestCount
    this.lastProcessedDigestCount = this.receivedDigestCount
  }

  async exchangeAndRepairRadix(peerAddress: string, radix: string): Promise<void> {
    const localEntry = this.radixEntries.get(radix)
    if (!localEntry) return

    try {
      // Exchange entries with peer
      const response: any = await postJson(`http://${peerAddress}/exchangeCheckpointRadixEntries`, {
        bucketID: this.bucketID,
        entries: [localEntry],
        checkpointType: this.checkpointType,
      })

      if (!response?.entries) {
        Logger.mainLogger.error('exchangeCheckpointRadixEntries invalid response:', response)
        return
      }

      const previousHash = localEntry.digest.hash

      // Process received entries
      this.onExchangeRadixEntries(this.bucketID, response.entries)
      // If our hash changed, update tally and share
      if (previousHash !== localEntry.digest.hash) {
        const tally = this.peerRadixDigests.get(radix)
        if (tally) {
          // Update hash tally counts
          const oldCount = tally.hashTally.get(previousHash) || 0
          if (oldCount > 0) {
            tally.hashTally.set(previousHash, oldCount - 1)
          }
          const newCount = tally.hashTally.get(localEntry.digest.hash) || 0
          tally.hashTally.set(localEntry.digest.hash, newCount + 1)
        }
        // Share our new state
        await this.shareRadixDigests([radix])
      }
    } catch (err) {
      Logger.mainLogger.error('Exchange and repair radix failed:', err)
    }
  }

  // We update the tallies for each radix.
  onHashDigestsReceived(
    senderAddress: string,
    bucketID: string,
    radixDigests: CheckpointRadixDigest[]
  ): void {
    try {
      if (bucketID !== this.bucketID) {
        Logger.mainLogger.debug(
          `[CheckpointBucket] onHashDigestsReceived: bucket mismatch, ignoring. Got ${bucketID}, expected ${this.bucketID}`
        )
        return
      }

      for (const digest of radixDigests) {
        let tally = this.peerRadixDigests.get(digest.radix)
        if (!tally) {
          tally = new RadixDigestTally(digest.radix)
          this.peerRadixDigests.set(digest.radix, tally)

          // Add our own entry to the tally if we have one
          const ourEntry = this.radixEntries.get(digest.radix)
          if (ourEntry) {
            // Initialize tally with our hash counted as 1
            tally.hashTally.set(ourEntry.digest.hash, 1) // TODO : might need rework on how the tally is incremented, might need to move it to the constructor
          }
        }

        // Get previous digest from this peer if it exists
        const previousDigest = tally.peerDigests.get(senderAddress) // this maps every archiver to a particular digest for the respective Checkpoint Bucket

        // If peer had a different hash before, decrement its count
        if (previousDigest && previousDigest.hash !== digest.hash) {
          const oldCount = tally.hashTally.get(previousDigest.hash) || 0
          if (oldCount > 0) {
            tally.hashTally.set(previousDigest.hash, oldCount - 1)
          }
        }

        // Update peer's digest
        tally.peerDigests.set(senderAddress, digest)

        // Update hash tally (our vote is already counted)
        const currentCount = tally.hashTally.get(digest.hash) || 0
        tally.hashTally.set(digest.hash, currentCount + 1)

        // Compare with our entry
        const ourEntry = this.radixEntries.get(digest.radix)
        if (ourEntry && ourEntry.digest.hash !== digest.hash) {
          this.hasUpdatesToShare = true
        }
      }

      this.receivedDigestCount++
    } catch (err) {
      Logger.mainLogger.error('Error in onHashDigestsReceived:', err)
    }
  }

  onExchangeRadixEntries(bucketID: string, entries: CheckpointRadixEntry<T>[]): void {
    try {
      if (bucketID !== this.bucketID) {
        Logger.mainLogger.debug(`Bucket mismatch, ignoring. Got ${bucketID}, expected ${this.bucketID}`)
        return
      }

      const updatedRadixes: string[] = []

      for (const incomingEntry of entries) {
        let localEntry = this.radixEntries.get(incomingEntry.digest.radix)
        if (!localEntry) {
          Logger.mainLogger.error(
            `onExchangeRadixEntries localEntry not found for ${incomingEntry.digest.radix} in bucket ${this.bucketID}`,
            incomingEntry
          )
          continue
        }

        const previousHash = localEntry.digest.hash
        let entryUpdated = false

        // Merge incoming data
        for (const data of incomingEntry.sortedData) {
          if (!localEntry.sortedData.find((d) => d.h === data.h)) {
            if (this.validateData && !this.validateData(data)) {
              Logger.mainLogger.error('Validation failed for data:', data)
              continue
            }

            localEntry.sortedData.push(data)
            entryUpdated = true

            if (this.updateData) {
              this.updateData(data).catch((err) => {
                Logger.mainLogger.error('Failed to persist data:', err)
              })
            }
          }
        }

        if (entryUpdated) {
          // Update our digest
          localEntry.updateDigest()

          // If hash changed, update tally
          if (previousHash !== localEntry.digest.hash) {
            const tally = this.peerRadixDigests.get(incomingEntry.digest.radix)
            if (tally) {
              // Decrement old hash count (including our previous vote)
              const oldCount = tally.hashTally.get(previousHash) || 1 // At least 1 for our vote
              if (oldCount > 1) {
                tally.hashTally.set(previousHash, oldCount - 1)
              } else {
                tally.hashTally.delete(previousHash) // Remove if it was just our vote
              }

              // Add our new hash with count of 1 (our vote)
              tally.hashTally.set(localEntry.digest.hash, 1)
            }
            updatedRadixes.push(incomingEntry.digest.radix)
          }
        }
      }

      // If we updated anything, share our new digests
      if (updatedRadixes.length > 0) {
        this.hasUpdatesToShare = true
        this.shareRadixDigests(updatedRadixes)
      }
    } catch (err) {
      Logger.mainLogger.error('Error in onExchangeRadixEntries:', err)
    }
  }

  toJSON() {
    try {
      return {
        startTime: this.startTime,
        endTime: this.endTime,
        bucketID: this.bucketID,
        checkpointType: this.checkpointType,
        hasUpdatesToShare: this.hasUpdatesToShare,
        sentDigestsCount: this.sentDigestsCount,
        receivedDigestCount: this.receivedDigestCount,
        lastProcessedDigestCount: this.lastProcessedDigestCount,
        BucketMatureAge: this.BucketMatureAge,
        GiveUpAge: this.GiveUpAge,
        // Convert Maps to objects
        radixEntries: Object.fromEntries(this.radixEntries),
        peerRadixDigests: Object.fromEntries(this.peerRadixDigests),
      }
    } catch (err) {
      Logger.mainLogger.error('Error in toJSON:', err)
      return null
    }
  }
}

// Use this to keep track of peers, include our tally in this too
export class RadixDigestTally {
  radix: string
  // key = digestHash, value = number of peers who reported it
  hashTally: Map<string, number>
  // key = peerAddress, value = the digest from that peer
  peerDigests: Map<string, CheckpointRadixDigest>

  constructor(radix: string) {
    this.radix = radix // aa
    this.hashTally = new Map<string, number>() // tracks the count of how many archivers contain the hash for a particular digest in the current CheckpointBucket
    this.peerDigests = new Map<string, CheckpointRadixDigest>() // maps a peer archiver to a radixDigest for the current CheckpointBucket
  }
}
