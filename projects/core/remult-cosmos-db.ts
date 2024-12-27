import type { Container } from '@azure/cosmos'
import { CosmosClient } from '@azure/cosmos'
import type {
  DataProvider,
  EntityDataProvider,
  EntityDataProviderFindOptions,
  EntityMetadata,
  FieldMetadata,
} from './index.js'
import { Filter } from './index.js'
import type { EntityDbNamesBase } from './src/filter/filter-consumer-bridge-to-sql-request.js'
import { dbNamesOf } from './src/filter/filter-consumer-bridge-to-sql-request.js'
import type { FilterConsumer } from './src/filter/filter-interfaces.js'
import { getRowAfterUpdate } from './src/data-providers/sql-database.js'
import type { EntityDataProviderGroupByOptions } from './src/data-interfaces.js'
import { GroupByOperators, GroupByCountMember } from './src/remult3/remult3.js'

export interface CosmosConfig {
  endpoint: string
  key: string
  databaseId: string
  containerName?: string
}

export class CosmosDataProvider implements DataProvider {
  public client: CosmosClient
  private container!: Container
  private initialized = false

  constructor(private config: CosmosConfig) {
    this.client = new CosmosClient({
      endpoint: config.endpoint,
      key: config.key,
    })
  }

  async init() {
    if (!this.initialized) {
      const { database } = await this.client.databases.createIfNotExists({
        id: this.config.databaseId,
      })
      const cName = this.config.containerName || 'default'
      this.container = (
        await database.containers.createIfNotExists({
          id: cName,
          partitionKey: { paths: ['/id'] },
        })
      ).container
      this.initialized = true
    }
  }

  async ensureSchema(entities: EntityMetadata[]): Promise<void> {
    if (!this.initialized) await this.init()
  }

  getEntityDataProvider(entity: EntityMetadata): EntityDataProvider {
    return new CosmosEntityDataProvider(this.container, entity)
  }

  async transaction(action: (dataProvider: DataProvider) => Promise<void>) {
    await action(this)
  }
}

class CosmosEntityDataProvider implements EntityDataProvider {
  constructor(
    private container: Container,
    private entity: EntityMetadata,
  ) {}

  async find(options: EntityDataProviderFindOptions): Promise<any[]> {
    const e = await dbNamesOf(this.entity)
    let query = 'SELECT * FROM c'
    const queryParams: any[] = []

    if (options?.where) {
      const filterBuilder = new FilterConsumerBridgeToCosmosQuery(e)
      options.where.__applyToConsumer(filterBuilder)
      const whereClause = await filterBuilder.resolveWhere()
      if (whereClause.query) {
        query += ` WHERE ${whereClause.query}`
        queryParams.push(...whereClause.parameters)
      }
    }

    if (options?.orderBy) {
      query +=
        ' ORDER BY ' +
        options.orderBy.Segments.map(
          (s) => `c.${e.$dbNameOf(s.field)} ${s.isDescending ? 'DESC' : 'ASC'}`,
        ).join(', ')
    }

    if (options?.limit) {
      query += ` OFFSET ${
        options.page ? (options.page - 1) * options.limit : 0
      } LIMIT ${options.limit}`
    }

    const { resources } = await this.container.items
      .query({
        query,
        parameters: queryParams.map((value, index) => ({
          name: `@p${index}`,
          value,
        })),
      })
      .fetchAll()

    return resources.map((doc) => this.translateFromDb(doc, e))
  }

  async count(where: Filter): Promise<number> {
    const e = await dbNamesOf(this.entity)
    const filterBuilder = new FilterConsumerBridgeToCosmosQuery(e)
    where.__applyToConsumer(filterBuilder)
    const whereClause = await filterBuilder.resolveWhere()

    const query = `SELECT VALUE COUNT(1) FROM c${
      whereClause.query ? ` WHERE ${whereClause.query}` : ''
    }`

    const { resources } = await this.container.items
      .query({
        query,
        parameters: whereClause.parameters.map((value, index) => ({
          name: `@p${index}`,
          value,
        })),
      })
      .fetchAll()

    return resources[0]
  }

  async insert(data: any): Promise<any> {
    const e = await dbNamesOf(this.entity)
    const doc = await this.translateToDb(data, e)
    const { resource } = await this.container.items.create(doc)
    return this.translateFromDb(resource, e)
  }

  async update(id: any, data: any): Promise<any> {
    const e = await dbNamesOf(this.entity)
    const filter = new FilterConsumerBridgeToCosmosQuery(e)
    Filter.fromEntityFilter(
      this.entity,
      this.entity.idMetadata.getIdFilter(id),
    ).__applyToConsumer(filter)
    const whereClause = await filter.resolveWhere()

    const { resources } = await this.container.items
      .query({
        query: `SELECT * FROM c WHERE ${whereClause.query}`,
        parameters: whereClause.parameters.map((value, index) => ({
          name: `@p${index}`,
          value,
        })),
      })
      .fetchAll()

    if (resources.length === 0) {
      throw new Error('Document not found')
    }

    const doc = resources[0]
    const updates = await this.translateToDb(data, e)
    Object.assign(doc, updates)

    const { resource } = await this.container.item(doc.id).replace(doc)
    return getRowAfterUpdate(this.entity, this, data, id, 'update')
  }

  async delete(id: any): Promise<void> {
    const e = await dbNamesOf(this.entity)
    const filter = new FilterConsumerBridgeToCosmosQuery(e)
    Filter.fromEntityFilter(
      this.entity,
      this.entity.idMetadata.getIdFilter(id),
    ).__applyToConsumer(filter)
    const whereClause = await filter.resolveWhere()

    const { resources } = await this.container.items
      .query({
        query: `SELECT * FROM c WHERE ${whereClause.query}`,
        parameters: whereClause.parameters.map((value, index) => ({
          name: `@p${index}`,
          value,
        })),
      })
      .fetchAll()

    if (resources.length > 0) {
      await this.container.item(resources[0].id).delete()
    }
  }

  async groupBy(options?: EntityDataProviderGroupByOptions): Promise<any[]> {
    const e = await dbNamesOf(this.entity)
    let pipeline: any[] = []

    // Handle where clause if it exists
    if (options?.where) {
      const filterBuilder = new FilterConsumerBridgeToCosmosQuery(e)
      options.where.__applyToConsumer(filterBuilder)
      const whereClause = await filterBuilder.resolveWhere()
      if (whereClause.query) {
        pipeline.push({
          query: `SELECT * FROM c WHERE ${whereClause.query}`,
          parameters: whereClause.parameters.map((value, index) => ({
            name: `@p${index}`,
            value,
          })),
        })
      }
    }

    // Build group by query
    let groupQuery = 'SELECT '
    const processResultRow: ((cosmosRow: any, resultRow: any) => void)[] = []

    // Add count
    groupQuery += 'COUNT(1) as __count'
    processResultRow.push((cosmosRow, resultRow) => {
      resultRow[GroupByCountMember] = cosmosRow.__count
    })

    // Handle group by fields
    if (options?.group) {
      groupQuery +=
        ', ' +
        options.group
          .map((field) => {
            const dbField = e.$dbNameOf(field)
            processResultRow.push((cosmosRow, resultRow) => {
              resultRow[field.key] = field.valueConverter.fromDb(
                cosmosRow[dbField],
              )
            })
            return `c.${dbField}`
          })
          .join(', ')
    }

    // Handle aggregations
    for (const operator of GroupByOperators) {
      if (options?.[operator]) {
        for (const field of options[operator]) {
          const dbField = e.$dbNameOf(field)
          const alias = `${dbField}_${operator}`

          if (operator === 'distinctCount') {
            groupQuery += `, COUNT(DISTINCT c.${dbField}) as ${alias}`
          } else {
            groupQuery += `, ${operator.toUpperCase()}(c.${dbField}) as ${alias}`
          }

          processResultRow.push((cosmosRow, resultRow) => {
            resultRow[field.key] = {
              ...resultRow[field.key],
              [operator]: cosmosRow[alias],
            }
          })
        }
      }
    }

    // Add FROM and GROUP BY
    groupQuery += ' FROM c'
    if (options?.group) {
      groupQuery +=
        ' GROUP BY ' +
        options.group.map((field) => `c.${e.$dbNameOf(field)}`).join(', ')
    }

    // Handle order by
    if (options?.orderBy) {
      groupQuery +=
        ' ORDER BY ' +
        options.orderBy
          .map((x) => {
            const direction = x.isDescending ? 'DESC' : 'ASC'
            switch (x.operation) {
              case 'count':
                return `__count ${direction}`
              case undefined:
                return `c.${e.$dbNameOf(x.field!)} ${direction}`
              default:
                return `${e.$dbNameOf(x.field!)}_${x.operation} ${direction}`
            }
          })
          .join(', ')
    }

    // Handle pagination
    if (options?.limit) {
      const offset = options.page ? (options.page - 1) * options.limit : 0
      groupQuery += ` OFFSET ${offset} LIMIT ${options.limit}`
    }

    // Execute query
    const { resources } = await this.container.items
      .query({
        query: groupQuery,
      })
      .fetchAll()

    // Process results
    return resources.map((row) => {
      const resultRow: any = {}
      for (const processor of processResultRow) {
        processor(row, resultRow)
      }
      return resultRow
    })
  }

  private translateFromDb(doc: any, nameProvider: EntityDbNamesBase) {
    const result: any = {}
    for (const field of this.entity.fields) {
      const dbName = nameProvider.$dbNameOf(field)
      let value = doc[dbName]

      if (value === undefined) {
        if (!field.allowNull) {
          value = field.valueType === String ? '' : null
        } else {
          value = null
        }
      }

      result[field.key] = field.valueConverter.fromDb(value)
    }
    return result
  }

  private async translateToDb(data: any, nameProvider: EntityDbNamesBase) {
    const result: any = {}
    for (const field of this.entity.fields) {
      if (!field.dbReadOnly && !field.isServerExpression) {
        const value = data[field.key]
        if (value !== undefined) {
          result[nameProvider.$dbNameOf(field)] =
            field.valueConverter.toDb(value)
        }
      }
    }
    return result
  }
}

class FilterConsumerBridgeToCosmosQuery implements FilterConsumer {
  private conditions: { query: string; parameters: any[] }[] = []

  constructor(private nameProvider: EntityDbNamesBase) {}

  async resolveWhere() {
    if (this.conditions.length === 0) {
      return { query: '', parameters: [] }
    }

    const query = this.conditions.map((c) => `(${c.query})`).join(' AND ')
    const parameters = this.conditions.flatMap((c) => c.parameters)
    return { query, parameters }
  }

  or(orElements: Filter[]): void {
    const orConditions: { query: string; parameters: any[] }[] = []

    for (const element of orElements) {
      const filter = new FilterConsumerBridgeToCosmosQuery(this.nameProvider)
      element.__applyToConsumer(filter)
      const clause = filter.conditions[0]
      if (clause) {
        orConditions.push(clause)
      }
    }

    if (orConditions.length > 0) {
      const query = orConditions.map((c) => `(${c.query})`).join(' OR ')
      const parameters = orConditions.flatMap((c) => c.parameters)
      this.conditions.push({ query, parameters })
    }
  }

  not(element: Filter): void {
    const filter = new FilterConsumerBridgeToCosmosQuery(this.nameProvider)
    element.__applyToConsumer(filter)
    const clause = filter.conditions[0]
    if (clause) {
      this.conditions.push({
        query: `NOT (${clause.query})`,
        parameters: clause.parameters,
      })
    }
  }

  isNull(field: FieldMetadata): void {
    const dbName = this.nameProvider.$dbNameOf(field)
    this.conditions.push({
      query: `IS_NULL(c.${dbName})`,
      parameters: [],
    })
  }

  isNotNull(field: FieldMetadata): void {
    const dbName = this.nameProvider.$dbNameOf(field)
    this.conditions.push({
      query: `IS_DEFINED(c.${dbName}) AND NOT IS_NULL(c.${dbName})`,
      parameters: [],
    })
  }

  isEqualTo(field: FieldMetadata, value: any): void {
    this.addCondition(field, '=', value)
  }

  isDifferentFrom(field: FieldMetadata, value: any): void {
    this.addCondition(field, '!=', value)
  }

  isGreaterOrEqualTo(field: FieldMetadata, value: any): void {
    this.addCondition(field, '>=', value)
  }

  isGreaterThan(field: FieldMetadata, value: any): void {
    this.addCondition(field, '>', value)
  }

  isLessOrEqualTo(field: FieldMetadata, value: any): void {
    this.addCondition(field, '<=', value)
  }

  isLessThan(field: FieldMetadata, value: any): void {
    this.addCondition(field, '<', value)
  }

  isIn(field: FieldMetadata, values: any[]): void {
    const dbName = this.nameProvider.$dbNameOf(field)
    const paramNames = values.map((_, i) => `@p${this.conditions.length}_${i}`)

    this.conditions.push({
      query: `c.${dbName} IN (${paramNames.join(', ')})`,
      parameters: values.map((value) => field.valueConverter.toDb(value)),
    })
  }

  containsCaseInsensitive(field: FieldMetadata, value: any): void {
    const dbName = this.nameProvider.$dbNameOf(field)
    this.conditions.push({
      query: `CONTAINS(LOWER(c.${dbName}), @p${this.conditions.length})`,
      parameters: [field.valueConverter.toDb(value.toLowerCase())],
    })
  }

  notContainsCaseInsensitive(field: FieldMetadata, value: any): void {
    const dbName = this.nameProvider.$dbNameOf(field)
    this.conditions.push({
      query: `NOT CONTAINS(LOWER(c.${dbName}), @p${this.conditions.length})`,
      parameters: [field.valueConverter.toDb(value.toLowerCase())],
    })
  }

  startsWithCaseInsensitive(field: FieldMetadata, value: any): void {
    const dbName = this.nameProvider.$dbNameOf(field)
    this.conditions.push({
      query: `STARTSWITH(LOWER(c.${dbName}), @p${this.conditions.length})`,
      parameters: [field.valueConverter.toDb(value.toLowerCase())],
    })
  }

  endsWithCaseInsensitive(field: FieldMetadata, value: any): void {
    const dbName = this.nameProvider.$dbNameOf(field)
    this.conditions.push({
      query: `ENDSWITH(LOWER(c.${dbName}), @p${this.conditions.length})`,
      parameters: [field.valueConverter.toDb(value.toLowerCase())],
    })
  }

  custom(key: string, customItem: any): void {
    throw new Error(
      'Custom filter is not supported in Cosmos DB implementation',
    )
  }

  databaseCustom(databaseCustom: any): void {
    throw new Error(
      'Database custom filter is not supported in Cosmos DB implementation',
    )
  }

  private addCondition(field: FieldMetadata, operator: string, value: any) {
    const dbName = this.nameProvider.$dbNameOf(field)
    const paramName = `@p${this.conditions.length}`
    this.conditions.push({
      query: `c.${dbName} ${operator} ${paramName}`,
      parameters: [field.valueConverter.toDb(value)],
    })
  }
}

export async function createCosmosConnection(config: CosmosConfig) {
  const provider = new CosmosDataProvider(config)
  await provider.init()
  return provider
}
