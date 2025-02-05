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

  // addData(data: CheckpointData<T>): void {
  //   console.log('[check-point] CheckpointRadixEntry addData', data)
  //   this.sortedData.push(data)
  //   console.log('[check-point] addData starting updateDigest')

  //   // Update digest after modifying data
  //   this.updateDigest()
  //   console.log('[check-point] CheckpointRadixEntry updateDigest and addData end')
  // }

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

  constructor(private persistenceCallbacks: DataPersistenceCallbacks<T>, checkpointType: CheckpointType) {
    this.checkpointBuckets = new Map<string, CheckpointBucket<T>>()
    this.validateData = persistenceCallbacks.validateData
    this.updateData = persistenceCallbacks.updateData
    this.checkpointType = checkpointType
  }

  addData(data: CheckpointData<T>, bucketID: string): void {
    console.log('[check-point] CheckpointBucketManager addData', data, bucketID)
    let bucket = this.checkpointBuckets.get(bucketID)
    if (!bucket) {
      console.log('[check-point] CheckpointBucketManager addData creating new bucket')
      const startTime = Math.floor(data.t)
      const endTime = startTime + 60 // for 1 minute buckets
      bucket = new CheckpointBucket<T>(startTime, endTime, bucketID, this.validateData, this.updateData, this.checkpointType)
      this.checkpointBuckets.set(bucketID, bucket) // adding an entry that maps the CheckpointType object to its contents, the key here is the address
    }
    console.log('[check-point] CheckpointBucketManager addData adding data to bucket')
    bucket.addData(data)
    console.log('[check-point] CheckpointBucketManager addData end')
  }

  // Periodically update all buckets to indicate the lifetime of the bucket since inserting into local memory
  async update(): Promise<void> {
    // update the naming convention here to indicate time change and not data updation
    console.log('[check-point] CheckpointBucketManager update')
    const currentTime = Math.floor(Date.now() / 1000)
    const toRemove: string[] = []
    for (const [id, bucket] of this.checkpointBuckets.entries()) {
      if (!bucket) {
        continue
      }

      const age = currentTime - bucket.startTime
      if (age > bucket.GiveUpAge) {
        // We consider this bucket "failed" or "too old" => persist & alert
        console.log(
          `[CheckpointBucketManager] Bucket ${bucket.bucketID} exceeded GiveUpAge. Persisting & removing.`
        )
        // TODO : persist and alert
        if (bucket.hasUpdatesToShare) {
          bucket.writeToFileAndAlert()
        } else {
          console.log(
            '[check-point] CheckpointBucketManager is older than giveUpAge and has no updates to share',
            bucket.bucketID
          )

          if (bucket.updateData) {
            const promises: Promise<void>[] = []
            for (const entry of bucket.radixEntries.values()) {
              for (const dataItem of entry.sortedData) {
                promises.push(bucket.updateData(dataItem))
              }
            }
            await Promise.all(promises)
          }
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
    console.log('[check-point] CheckpointBucketManager update end')
  }

  onHashDigestsReceived(
    // receives metadata about the radix information for a particular class type, recieves only radix prefix, count of data and hash, doesnt recieve actual payload
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
    // receives a list of entries which contain radix metadata ( radixDigest ) and the payload for a respective radix ( radix Sorted Data )
    console.log('[check-point] CheckpointBucketManager onExchangeRadixEntries', bucketID, entries)
    const bucket = this.checkpointBuckets.get(bucketID)
    if (!bucket) {
      Logger.mainLogger.error(
        `[CheckpointBucketManager] onExchangeRadixEntries: no bucket found for ID=${bucketID}`
      )
      return []
    }

    console.log('[check-point] CheckpointBucketManager onExchangeRadixEntries adding data to bucket')
    // First, pass the incoming entries to the bucket for merging.
    bucket.onExchangeRadixEntries(bucketID, entries)
    console.log('[check-point] CheckpointBucketManager onExchangeRadixEntries merging done')

    // Then, gather only the radixes actually included in the incoming entries
    // so that we return updated entries reflecting any newly merged data.
    const result: CheckpointRadixEntry<T>[] = []
    for (const incomingEntry of entries) {
      const localEntry = bucket.radixEntries.get(incomingEntry.digest.radix)
      if (!localEntry) {
        continue
      }
      // Update our digest now that we've potentially merged new data.
      localEntry.updateDigest()
      result.push(localEntry)
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
      // Should not occur as we have pre initialized radix entries
      console.error('[check-point] CheckpointBucket addData Radix not found')
      return
    }

    console.log('[check-point] CheckpointBucket addData adding data to CheckpointRadixEntry')
    // Add data to memory
    entry.sortedData.push(data)
    this.hasUpdatesToShare = true

    // // Persist to storage via callback
    // try {
    //   await this.updateData(data)
    // } catch (err) {
    //   console.error('[check-point] CheckpointBucket addData failed to persist data', err)
    //   Logger.mainLogger.error('[CheckpointBucket] Failed to persist data:', err)
    //   // Optionally: roll back memory update if persistence fails
    //   // entry.removeData(data)
    //   throw err
    // }
    console.log('[check-point] CheckpointBucket addData end')
  }

  //gets the digest for each radixEntry, puts these in a list and
  //calls the shareCheckpointRadixDigests endpoint for each node
  //the radix entry must updateDigest first
  //if radixList is specified then only share the specified radix strings, otherwise share them all
  //increment sentDigestsCount
  async shareRadixDigests(radixList?: string[]): Promise<void> {
    console.log('[check-point] CheckpointBucket shareRadixDigests', radixList)

    // Only share if we have updates pending
    if (!this.hasUpdatesToShare) {
      console.log('[check-point] CheckpointBucket shareRadixDigests no updates to share')
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
      console.log(
        '[check-point] CheckpointBucket shareRadixDigests no digests to share for the specified radix list'
      )
      this.hasUpdatesToShare = false
      return
    }

    // Gather a list of peers from our configuration
    const peers = otherArchivers.map((archiver) => `${archiver.ip}:${archiver.port}`)

    // Send digests to all peers
    const sharePromises = peers.map((peerAddress) =>
      postJson(`http://${peerAddress}/shareCheckpointRadixDigests`, {
        senderAddress: `${config.ARCHIVER_IP}:${config.ARCHIVER_PORT}`,
        bucketID: this.bucketID,
        radixDigests: digests,
        checkpointType: this.checkpointType
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
      // Wait for all share attempts to complete (successful or not)
      await Promise.allSettled(sharePromises)
      this.sentDigestsCount++

      // If at least one share attempt happened, we can reset hasUpdatesToShare.
      // If subsequent repairs occur, hasUpdatesToShare will be set back to true elsewhere.
      if (sharePromises.length > 0) {
        this.hasUpdatesToShare = false
      }
    } catch (err) {
      console.error('[check-point] CheckpointBucket shareRadixDigests failed to share digests', err)
      Logger.mainLogger.error('Error in shareRadixDigests:', err)
    }
  }

  update(currentTime: number): void {
    console.log('[check-point] CheckpointBucket update', currentTime)
    const bucketAge = currentTime - this.startTime

    // Check for give up condition (20 minutes)
    if (bucketAge > config.checkpointBucketConfig.GiveUpAge) {
      console.log('[check-point] CheckpointBucket update giving up')
      //TODO: if no updates to share then we save else we write to file and alert
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
      // TODO : are we evaluating consensus for digests i.e for multiple radices or just for one particular radix
      // should be done for the all radixDigests present for a bucketId
      // needs re-evaluation
      this.evaluateDigestConsensus()
      console.log('[check-point] CheckpointBucket update evaluating digest consensus end')
    }
    console.log('[check-point] CheckpointBucket update end')
  }

  public writeToFileAndAlert(): void {
    // TODO : dont see any alerting going on over here.
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

  // look at each of our radix digest hashes.
  // for any of these where hashTalley for our hash is less than half
  // of the entries then we will call exchangeAndRepairRadix with a random
  // node that has a different hash than us for the given radix
  // set lastProcessedDigestCount to receivedDigestCount when done
  evaluateDigestConsensus(): void {
    console.log('[check-point] CheckpointBucket evaluateDigestConsensus')

    // If we have no peer data, nothing to evaluate
    if (this.peerRadixDigests.size === 0) {
      this.lastProcessedDigestCount = this.receivedDigestCount
      return
    }

    // Calculate bucket age in seconds. Ensure startTime is also stored in seconds.
    const currentTimeSec = Math.floor(Date.now() / 1000)
    const bucketAgeSec = currentTimeSec - this.startTime

    // totalArchivers counts ourself (+1) and external peers
    const totalArchivers = otherArchivers.length + 1
    // "Majority" means more than half.
    // e.g., if totalArchivers = 5, the threshold is 3
    const majorityThreshold = Math.floor(totalArchivers / 2) + 1

    console.log('[check-point] CheckpointBucket evaluateDigestConsensus iterating over peerRadixDigests')
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
            Logger.mainLogger.error(
              `[CheckpointBucket] Failed to repair radix ${radix} with peer ${randomPeer}:`,
              err
            )
          })
        }
      }
    }

    // After evaluating all radixes, update the lastProcessedDigestCount
    this.lastProcessedDigestCount = this.receivedDigestCount
    console.log('[check-point] CheckpointBucket evaluateDigestConsensus end')
  }

  // we will call the endpoint exhangeCheckpointRadixEntries
  // if this results in us updating our own state in any way, then
  // we must update our own digest, and the related RadixDigestTally
  // at the end, call shareRadixDigests(radixList) for any that we update to share our updated value with peers
  // this will need to call some the integration related functions listed below
  //  todo decide how to inject these.. probably just as variables.
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
        checkpointType: this.checkpointType
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

  // update peerRadixDigests    increment receivedDigestCount
  //Handler for incoming hash digests from a peer.
  // We update the tallies for each radix.
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
    // TODO : might need rework
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
    console.log('[check-point] CheckpointBucket onHashDigestsReceived end')
  }

  /**
   * Handler for incoming data entries from a peer (repair process).
   * We compare and merge.
   */
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
        console.error(
          '[check-point] CheckpointBucket onExchangeRadixEntries localEntry not found',
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
            Logger.mainLogger.error('[CheckpointBucket] Validation failed for data:', data)
            continue
          }

          console.log('[check-point] CheckpointBucket onExchangeRadixEntries adding data to localEntry')
          localEntry.sortedData.push(data)
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
