import { describe, it, beforeAll, afterAll, expect } from 'vitest'
import { Entity, Fields, Remult, Repository } from '../core'
import { CosmosDataProvider } from '../core/remult-cosmos-db.js'
import { GroupByCountMember } from '../core/src/remult3/remult3.js'

@Entity('events-monitor-dev')
class EventsMonitorDev {
  @Fields.uuid() id = ''

  @Fields.string()
  name = ''

  @Fields.string()
  eventType = ''
}

describe('Cosmos DB Data Provider', () => {
  let remult: Remult
  let repo: Repository<EventsMonitorDev>
  let provider: CosmosDataProvider

  beforeAll(async () => {
    provider = new CosmosDataProvider({
      endpoint: process.env.VITE_COSMOS_ENDPOINT || '',
      key: process.env.VITE_COSMOS_KEY || '',
      databaseId: process.env.VITE_COSMOS_DATABASE_ID || '',
      containerName: process.env.VITE_COSMOS_CONTAINER_NAME || '',
    })

    try {
      await provider.init()
      console.log('Provider initialized successfully')

      remult = new Remult()
      remult.dataProvider = provider
      repo = remult.repo(EventsMonitorDev)
    } catch (error) {
      console.error('Setup failed:', error)
      throw error
    }
  })

  afterAll(async () => {
    if (provider.client) {
      await provider.client.dispose()
      console.log('Provider connection closed')
    }
  })

  // Basic connectivity test
  it('should initialize the provider', () => {
    expect(provider).toBeDefined()
    expect(remult).toBeDefined()
    expect(repo).toBeDefined()
  })

  // Test basic data fetching
  it('should fetch data from Cosmos DB', async () => {
    try {
      console.log('Starting data fetch test')

      const results = await repo.find({
        limit: 5,
      })

      console.log(`Found ${results.length} records`)
      expect(Array.isArray(results)).toBe(true)

      if (results.length > 0) {
        const firstRecord = results[0]
        expect(firstRecord).toHaveProperty('id')
        expect(firstRecord).toHaveProperty('name')
        expect(firstRecord).toHaveProperty('eventType')
        console.log('Sample record:', firstRecord)
      }
    } catch (error) {
      console.error('Test failed:', error)
      throw error
    }
  })

  // Test insert operations
  it('should insert a new record', async () => {
    try {
      console.log('Starting insert test')

      const testData = {
        name: 'Test Event',
        eventType: 'test.event',
      }

      const newEvent = await repo.insert(testData)
      console.log('Inserted event:', newEvent)

      expect(newEvent.id).toBeDefined()
      expect(typeof newEvent.id).toBe('string')
      expect(newEvent.name).toBe('Test Event')
      expect(newEvent.eventType).toBe('test.event')

      const found = await repo.findFirst({ id: newEvent.id })
      expect(found).toBeDefined()
      expect(found?.id).toBe(newEvent.id)
    } catch (error) {
      console.error('Insert test failed:', error)
      throw error
    }
  })

  // Test update operations
  it('should update an existing record', async () => {
    try {
      console.log('Starting update test')

      const existing = await repo.findFirst({ eventType: 'test.event' })
      console.log('Found existing record:', existing)

      if (existing) {
        existing.name = 'Updated Event'
        existing.eventType = 'test.event.updated'
        const updated = await repo.save(existing)

        console.log('Updated record:', updated)
        expect(updated).toBeDefined()
        expect(updated.id).toBe(existing.id)
        expect(updated.name).toBe('Updated Event')
        expect(updated.eventType).toBe('test.event.updated')
      } else {
        console.warn('No record found for update test')
      }
    } catch (error) {
      console.error('Update test failed:', error)
      throw error
    }
  })

  // Test delete operations
  it('should delete an existing record', async () => {
    try {
      console.log('Starting delete test')

      const existing = await repo.findFirst({ eventType: 'test.event.updated' })
      console.log('Found existing record:', existing)

      if (existing) {
        await repo.delete(existing)
        const deleted = await repo.findFirst({ id: existing.id })
        expect(deleted).toBeUndefined()
      } else {
        console.warn('No record found for delete test')
      }
    } catch (error) {
      console.error('Delete test failed:', error)
      throw error
    }
  })

  // Test group by operations
  it('should support group by operations', async () => {
    try {
      console.log('Starting group by test')

      // Basic groupBy test
      const results = await repo.groupBy({
        group: ['eventType'], // Use string key instead of FieldMetadata
      })

      console.log('Group by results:', results)

      expect(Array.isArray(results)).toBe(true)
      if (results.length > 0) {
        expect(results[0]).toHaveProperty('eventType')
        expect(results[0]).toHaveProperty(GroupByCountMember)
      }

      // Test with a where clause
      const resultsWithFilter = await repo.groupBy({
        group: ['eventType'],
        where: { eventType: 'test.event' },
      })

      expect(Array.isArray(resultsWithFilter)).toBe(true)

      // Test with count
      const resultsWithCount = await repo.groupBy({
        group: ['eventType'],
      })

      if (resultsWithCount.length > 0) {
        expect(resultsWithCount[0]).toHaveProperty(GroupByCountMember)
      }
    } catch (error) {
      console.error('Group by test failed:', error)
      throw error
    }
  })
})
