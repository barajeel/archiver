import * as fs from 'fs'
import * as Logger from '../Logger'
import { otherArchivers } from '../State'
import { postJson } from '../P2P'
import { config } from '../Config'

export enum CheckpointType {
  Cycle = 0,
  Transaction = 1,
  Receipt = 2,
  // Account = 3,
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

  addData(data: CheckpointData<T>): void {
    console.log('[check-point] CheckpointRadixEntry addData', data)
    // Find the correct position to insert while maintaining sort by address
    const insertIndex = this.sortedData.findIndex((item) => item.a > data.a)

    if (insertIndex === -1) {
      // Add to end if no larger address found
      this.sortedData.push(data)
    } else {
      // Insert at correct position
      this.sortedData.splice(insertIndex, 0, data)
    }

    console.log('[check-point] addData starting updateDigest')

    // Update digest after modifying data
    this.updateDigest()
    console.log('[check-point] CheckpointRadixEntry updateDigest and addData end')
  }

  updateDigest(): void {
    // Sort data by address if not already sorted
    this.sortedData.sort((a, b) => a.a.localeCompare(b.a))

    // Update the digest based on the sorted data
    this.digest.hash = this.computeHash()
    this.digest.itemCount = this.sortedData.length
  }

  private computeHash(): string {
    // Ensure data is sorted before computing hash
    return this.sortedData
      .map((d) => d.h) // Use the unique hash/identifier
      .join('') // Join all hashes together
  }
}

export interface DataPersistenceCallbacks<T> {
  updateData: (data: CheckpointData<T>) => Promise<void>
  validateData: (data: CheckpointData<T>) => Promise<boolean>
  loadBucket?: (bucketID: string) => Promise<CheckpointBucket<T> | null>
}

// Manages all buckets, routes incoming data to the correct bucket, and does periodic updates.
export class CheckpointBucketManager<T> {
  checkpointBuckets: Map<string, CheckpointBucket<T>>
  validateData: (data: CheckpointData<T>) => Promise<boolean>
  updateData: (data: CheckpointData<T>) => Promise<void>

  constructor(private persistenceCallbacks: DataPersistenceCallbacks<T>) {
    this.checkpointBuckets = new Map<string, CheckpointBucket<T>>()
    this.validateData = persistenceCallbacks.validateData
    this.updateData = persistenceCallbacks.updateData
  }

  addData(data: CheckpointData<T>, bucketID: string): void {
    console.log('[check-point] CheckpointBucketManager addData', data, bucketID)
    let bucket = this.checkpointBuckets.get(bucketID)
    if (!bucket) {
      console.log('[check-point] CheckpointBucketManager addData creating new bucket')
      const startTime = Math.floor(data.t)
      const endTime = startTime + 60 // for 1 minute buckets
      bucket = new CheckpointBucket<T>(startTime, endTime, bucketID, this.validateData, this.updateData)
      this.checkpointBuckets.set(bucketID, bucket)
    }
    console.log('[check-point] CheckpointBucketManager addData adding data to bucket')
    bucket.addData(data)
    console.log('[check-point] CheckpointBucketManager addData end')
  }

  // Periodically update all buckets
  update(): void {
    console.log('[check-point] CheckpointBucketManager update')
    const currentTime = Math.floor(Date.now() / 1000)
    for (const bucket of this.checkpointBuckets.values()) {
      bucket.update(currentTime)
    }
    console.log('[check-point] CheckpointBucketManager update end')
  }

  onHashDigestsReceived(
    senderAddress: string,
    bucketID: string,
    radixDigests: CheckpointRadixDigest[]
  ): void {
    console.log(
      '[check-point] CheckpointBucketManager onHashDigestsReceived',
      senderAddress,
      bucketID,
      radixDigests
    )
    const bucket = this.checkpointBuckets.get(bucketID)
    if (bucket) {
      console.log('[check-point] CheckpointBucketManager onHashDigestsReceived adding data to bucket')
      bucket.onHashDigestsReceived(senderAddress, bucketID, radixDigests)
      console.log('[check-point] CheckpointBucketManager onHashDigestsReceived end')
    }
  }

  onExchangeRadixEntries(bucketID: string, entries: CheckpointRadixEntry<T>[]): CheckpointRadixEntry<T>[] {
    console.log('[check-point] CheckpointBucketManager onExchangeRadixEntries', bucketID, entries)
    const bucket = this.checkpointBuckets.get(bucketID)
    if (!bucket) {
      Logger.mainLogger.error(
        `[CheckpointBucketManager] onExchangeRadixEntries: no bucket found for ID=${bucketID}`
      )
      return []
    }

    console.log('[check-point] CheckpointBucketManager onExchangeRadixEntries adding data to bucket')
    bucket.onExchangeRadixEntries(bucketID, entries)
    console.log('[check-point] CheckpointBucketManager onExchangeRadixEntries end')

    const result: CheckpointRadixEntry<T>[] = []
    for (const r of bucket.radixEntries.keys()) {
      const entry = bucket.radixEntries.get(r)
      if (entry) {
        entry.updateDigest()
        result.push(entry)
      }
    }
    console.log('[check-point] CheckpointBucketManager onExchangeRadixEntries result', result)
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
  repairedPeers: Map<string, Set<string>> = new Map()

  constructor(
    startTime: number,
    endTime: number,
    bucketID: string,
    validateData: (data: CheckpointData<T>) => Promise<boolean>,
    updateData: (data: CheckpointData<T>) => Promise<void>
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
  }

  async addData(data: CheckpointData<T>): Promise<void> {
    console.log('[check-point] CheckpointBucket addData', data)
    if (this.validateData) {
      const isValid = await this.validateData(data)
      if (!isValid) {
        console.error('[check-point] CheckpointBucket addData validation failed', data)
        Logger.mainLogger.error('[CheckpointBucket] Validation failed for data:', data)
        return
      }
    }

    const address = data.a.toLowerCase()
    const radix = address.substring(0, 2)

    let entry = this.radixEntries.get(radix)
    if (!entry) {
      console.error('[check-point] CheckpointBucket addData creating new CheckpointRadixEntry')
      entry = new CheckpointRadixEntry<T>(radix)
      this.radixEntries.set(radix, entry)
    }

    console.log('[check-point] CheckpointBucket addData adding data to CheckpointRadixEntry')
    // Add data to memory
    entry.addData(data)
    this.hasUpdatesToShare = true

    // Persist to storage via callback
    try {
      await this.updateData(data)
    } catch (err) {
      console.error('[check-point] CheckpointBucket addData failed to persist data', err)
      Logger.mainLogger.error('[CheckpointBucket] Failed to persist data:', err)
      // Optionally: roll back memory update if persistence fails
      // entry.removeData(data)
      throw err
    }
    console.log('[check-point] CheckpointBucket addData end')
  }

  update(currentTime: number): void {
    console.log('[check-point] CheckpointBucket update', currentTime)
    const bucketAge = currentTime - this.startTime

    // Check for give up condition (20 minutes)
    if (bucketAge > config.checkpointBucketConfig.GiveUpAge) {
      console.log('[check-point] CheckpointBucket update giving up')
      this.writeToFileAndAlert()
      return
    }

    // Check if bucket has matured (11 minutes) and has updates to share
    if (bucketAge > config.checkpointBucketConfig.BucketMatureAge && this.hasUpdatesToShare) {
      console.log('[check-point] CheckpointBucket update sharing radix digests')
      this.shareRadixDigests()
      console.log('[check-point] CheckpointBucket update sharing radix digests end')
    }

    // Check for consensus updates if we've received new digests since last processing
    // and we've sent at least one digest
    if (this.sentDigestsCount > 0 && this.receivedDigestCount > this.lastProcessedDigestCount) {
      console.log('[check-point] CheckpointBucket update evaluating digest consensus')
      this.evaluateDigestConsensus()
      console.log('[check-point] CheckpointBucket update evaluating digest consensus end')
    }
    console.log('[check-point] CheckpointBucket update end')
  }

  private writeToFileAndAlert(): void {
    try {
      console.log('[check-point] CheckpointBucket writeToFileAndAlert')
      const bucketData = {
        bucketID: this.bucketID,
        startTime: this.startTime,
        endTime: this.endTime,
        radixEntries: Array.from(this.radixEntries.entries()),
        peerDigests: Array.from(this.peerRadixDigests.entries()),
      }

      console.log('[check-point] CheckpointBucket writeToFileAndAlert bucketData', bucketData)
      // Write to file
      const filename = `failed-bucket-${this.bucketID}-${this.startTime}.json`
      fs.writeFileSync(filename, JSON.stringify(bucketData, null, 2))
      console.log('[check-point] CheckpointBucket writeToFileAndAlert end')
    } catch (err) {
      console.error('[check-point] CheckpointBucket writeToFileAndAlert error', err)
      Logger.mainLogger.error(`Bucket ${this.bucketID} failed to reach consensus after timeout.`)
    }
  }

  async shareRadixDigests(radixList?: string[]): Promise<void> {
    console.log('[check-point] CheckpointBucket shareRadixDigests', radixList)
    // Only share if we have updates to share
    if (!this.hasUpdatesToShare) {
      console.log('[check-point] CheckpointBucket shareRadixDigests no updates to share')
      return
    }

    const digests: CheckpointRadixDigest[] = []

    // Collect digests for all requested radixes (or all if none specified)
    for (const [radix, entry] of this.radixEntries) {
      if (!radixList || radixList.includes(radix)) {
        entry.updateDigest()
        digests.push(entry.digest)
      }
    }

    if (digests.length === 0) {
      console.log('[check-point] CheckpointBucket shareRadixDigests no digests to share')
      return
    }

    // Get list of peers from State.otherArchivers
    const peers = otherArchivers.map((archiver) => `${archiver.ip}:${archiver.port}`)

    // Share with all peers
    const sharePromises = peers.map((peerAddress) =>
      postJson(`http://${peerAddress}/shareCheckpointRadixDigests`, {
        senderAddress: `${config.ARCHIVER_IP}:${config.ARCHIVER_PORT}`,
        bucketID: this.bucketID,
        radixDigests: digests,
      }).catch((err) => {
        console.error(
          '[check-point] CheckpointBucket shareRadixDigests failed to share digests with peer',
          peerAddress,
          err
        )
        Logger.mainLogger.error(`Failed to share digests with peer ${peerAddress}:`, err)
      })
    )

    try {
      await Promise.allSettled(sharePromises)
      this.sentDigestsCount++

      // Only clear hasUpdatesToShare if we successfully shared with all peers
      if (sharePromises.length > 0) {
        this.hasUpdatesToShare = false
      }
    } catch (err) {
      console.error('[check-point] CheckpointBucket shareRadixDigests failed to share digests', err)
      Logger.mainLogger.error('Error in shareRadixDigests:', err)
    }
  }

  evaluateDigestConsensus(): void {
    console.log('[check-point] CheckpointBucket evaluateDigestConsensus')
    if (this.peerRadixDigests.size === 0) {
      this.lastProcessedDigestCount = this.receivedDigestCount
      return
    }

    // Get total number of archivers including self
    const totalArchivers = otherArchivers.length + 1
    const majorityThreshold = Math.floor(totalArchivers / 2) + 1

    console.log('[check-point] CheckpointBucket evaluateDigestConsensus iterating over peerRadixDigests')
    for (const [radix, tally] of this.peerRadixDigests) {
      const localEntry = this.radixEntries.get(radix)
      if (!localEntry) continue

      const ourHash = localEntry.digest.hash
      let ourHashCount = 1 // Start with 1 for our own vote

      // Count votes for our hash
      const ourTotalVotes = tally.hashTally.get(ourHash) || 0
      ourHashCount += ourTotalVotes
      const bucketAge = Date.now() - this.startTime

      // If we don't have majority
      if (ourHashCount < majorityThreshold) {
        // Get all peers we haven't tried yet
        const untriedPeers = Array.from(tally.peerDigests.entries())
          .filter(([peer, digest]) => {
            return digest.hash !== ourHash && !this.repairedPeers.get(radix)?.has(peer)
          })
          .map(([peer]) => peer)

        if (untriedPeers.length > 0) {
          // Pick random untried peer
          const randomPeer = untriedPeers[Math.floor(Math.random() * untriedPeers.length)]

          // Track that we tried this peer
          let repairedSet = this.repairedPeers.get(radix)
          if (!repairedSet) {
            repairedSet = new Set<string>()
            this.repairedPeers.set(radix, repairedSet)
          }
          repairedSet.add(randomPeer)

          // Attempt repair
          this.exchangeAndRepairRadix(randomPeer, radix).catch((err) => {
            Logger.mainLogger.error(
              `[CheckpointBucket] Failed to repair radix ${radix} with peer ${randomPeer}:`,
              err
            )
          })
        } else if (bucketAge > config.checkpointBucketConfig.GiveUpAge) {
          // If we've tried all peers and still no consensus, log for manual intervention
          Logger.mainLogger.error(
            `[CheckpointBucket] Failed to reach consensus for radix ${radix} after trying all peers. ` +
              `Our hash: ${ourHash}, Vote count: ${ourHashCount}/${totalArchivers}`
          )
        }
      }
    }

    this.lastProcessedDigestCount = this.receivedDigestCount
    console.log('[check-point] CheckpointBucket evaluateDigestConsensus end')
  }

  async exchangeAndRepairRadix(peerAddress: string, radix: string): Promise<void> {
    console.log('[check-point] CheckpointBucket exchangeAndRepairRadix', peerAddress, radix)
    const localEntry = this.radixEntries.get(radix)
    if (!localEntry) return

    try {
      console.log('[check-point] CheckpointBucket exchangeAndRepairRadix sending request')
      // Exchange entries with peer
      const response: any = await postJson(`http://${peerAddress}/exchangeCheckpointRadixEntries`, {
        bucketID: this.bucketID,
        entries: [localEntry],
      })

      if (!response?.entries) {
        console.error('[check-point] CheckpointBucket exchangeAndRepairRadix invalid response', response)
        Logger.mainLogger.error('Invalid response:', response)
        return
      }

      const previousHash = localEntry.digest.hash

      // Process received entries
      this.onExchangeRadixEntries(this.bucketID, response.entries)
      console.log('[check-point] CheckpointBucket exchangeAndRepairRadix end')
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
      console.error('[check-point] CheckpointBucket exchangeAndRepairRadix failed', err)
      Logger.mainLogger.error('Exchange failed:', err)
    }
  }

  onHashDigestsReceived(
    senderAddress: string,
    bucketID: string,
    radixDigests: CheckpointRadixDigest[]
  ): void {
    console.log('[check-point] CheckpointBucket onHashDigestsReceived', senderAddress, bucketID, radixDigests)
    if (bucketID !== this.bucketID) {
      console.error(
        '[check-point] CheckpointBucket onHashDigestsReceived bucket mismatch',
        bucketID,
        this.bucketID
      )
      Logger.mainLogger.debug(
        `[CheckpointBucket] onHashDigestsReceived: bucket mismatch, ignoring. Got ${bucketID}, expected ${this.bucketID}`
      )
      return
    }

    console.log('[check-point] CheckpointBucket onHashDigestsReceived iterating over radixDigests')
    for (const digest of radixDigests) {
      let tally = this.peerRadixDigests.get(digest.radix)
      if (!tally) {
        tally = new RadixDigestTally(digest.radix)
        this.peerRadixDigests.set(digest.radix, tally)

        // Add our own entry to the tally if we have one
        const ourEntry = this.radixEntries.get(digest.radix)
        if (ourEntry) {
          // Initialize tally with our hash counted as 1
          tally.hashTally.set(ourEntry.digest.hash, 1)
        }
      }

      // Get previous digest from this peer if it exists
      const previousDigest = tally.peerDigests.get(senderAddress)

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
    console.log('[check-point] CheckpointBucket onHashDigestsReceived end')
  }

  onExchangeRadixEntries(bucketID: string, entries: CheckpointRadixEntry<T>[]): void {
    console.log('[check-point] CheckpointBucket onExchangeRadixEntries', bucketID, entries)
    if (bucketID !== this.bucketID) {
      Logger.mainLogger.debug(
        `[CheckpointBucket] onExchangeRadixEntries: bucket mismatch, ignoring. Got ${bucketID}, expected ${this.bucketID}`
      )
      return
    }

    console.log('[check-point] CheckpointBucket onExchangeRadixEntries iterating over entries')
    const updatedRadixes: string[] = []

    console.log('[check-point] CheckpointBucket onExchangeRadixEntries iterating over entries')
    for (const incomingEntry of entries) {
      let localEntry = this.radixEntries.get(incomingEntry.digest.radix)
      if (!localEntry) {
        localEntry = new CheckpointRadixEntry<T>(incomingEntry.digest.radix)
        this.radixEntries.set(incomingEntry.digest.radix, localEntry)
      }

      const previousHash = localEntry.digest.hash
      let entryUpdated = false

      // Merge incoming data
      for (const data of incomingEntry.sortedData) {
        if (!localEntry.sortedData.find((d) => d.h === data.h)) {
          if (this.validateData && !this.validateData(data)) {
            Logger.mainLogger.error('[CheckpointBucket] Validation failed for data:', data)
            continue
          }

          console.log('[check-point] CheckpointBucket onExchangeRadixEntries adding data to localEntry')
          localEntry.addData(data)
          entryUpdated = true
          console.log('[check-point] CheckpointBucket onExchangeRadixEntries end')

          if (this.updateData) {
            this.updateData(data).catch((err) => {
              Logger.mainLogger.error('[CheckpointBucket] Failed to persist data:', err)
            })
          }
        }
      }

      if (entryUpdated) {
        console.log('[check-point] CheckpointBucket onExchangeRadixEntries updating digest')
        // Update our digest
        localEntry.updateDigest()
        console.log('[check-point] CheckpointBucket onExchangeRadixEntries end')

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

    console.log('[check-point] CheckpointBucket onExchangeRadixEntries updatedRadixes', updatedRadixes)
    // If we updated anything, share our new digests
    if (updatedRadixes.length > 0) {
      this.hasUpdatesToShare = true
      this.shareRadixDigests(updatedRadixes)
    }
    console.log('[check-point] CheckpointBucket onExchangeRadixEntries end')
  }
}

// Use this to keep track of peers, include our tally in this too
export class RadixDigestTally {
  radix: string
  hashTally: Map<string, number>
  peerDigests: Map<string, CheckpointRadixDigest>

  constructor(radix: string) {
    this.radix = radix
    this.hashTally = new Map<string, number>()
    this.peerDigests = new Map<string, CheckpointRadixDigest>()
  }
}
