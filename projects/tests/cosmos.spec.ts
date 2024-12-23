import { describe, it, beforeAll, afterAll, expect } from 'vitest'
import { Entity, Fields, Remult, Repository, SqlDatabase } from '../core'
import { CosmosDataProvider } from '../core/remult-cosmos-db.js'

// Add table entity information below for test mock
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
      remult.dataProvider = new SqlDatabase(provider)
      repo = remult.repo(EventsMonitorDev)
    } catch (error) {
      console.error('Setup failed:', error)
      throw error
    }
  })

  afterAll(async () => {
    if (provider) {
      await provider.end()
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

      // Simple find with limit
      const results = await repo.find({
        limit: 5,
      })

      console.log(`Found ${results.length} records`)

      // Basic validation
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

  it('should insert a new record', async () => {
    try {
      console.log('Starting insert test')

      const testData = {
        name: 'Test Event',
        eventType: 'test.event',
      }

      console.log('Inserting test data:', testData)

      const newEvent = await repo.insert(testData)
      console.log('Inserted event:', newEvent)

      // Validate the inserted data
      expect(newEvent.id).toBeDefined()
      expect(typeof newEvent.id).toBe('string')
      expect(newEvent.name).toBe('Test Event')
      expect(newEvent.eventType).toBe('test.event')

      // Verify we can fetch the inserted record
      const found = await repo.findFirst({ id: newEvent.id })

      expect(found).toBeDefined()
      expect(found?.id).toBe(newEvent.id)
    } catch (error) {
      console.error('Insert test failed:', error)
      throw error
    }
  })

  // Update a record
  it('should update an existing record', async () => {
    try {
      console.log('Starting update test')

      // Find an existing record
      const existing = await repo.findFirst({ eventType: 'test.event' })

      console.log('Found existing record:', existing)

      if (existing) {
        // Update the record
        existing.name = 'Updated Event'
        existing.eventType = 'test.event.updated'
        const updated = await repo.save(existing)

        console.log('Updated record:', updated)

        // Verify the update
        expect(updated).toBeDefined()
        expect(updated.id).toBe(existing.id)
        expect(updated.name).toBe('Updated Event')
      } else {
        console.warn('No record found for update test')
      }
    } catch (error) {
      console.error('Update test failed:', error)
      throw error
    }
  })

  // Delete a record
  it('should delete an existing record', async () => {
    try {
      console.log('Starting delete test')

      // Find an existing record
      const existing = await repo.findFirst({ eventType: 'test.event.updated' })

      console.log('Found existing record:', existing)

      if (existing) {
        // Delete the record
        await repo.delete(existing)

        // Verify the deletion
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
})
