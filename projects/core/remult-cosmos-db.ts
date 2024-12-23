import type { Container } from '@azure/cosmos'
import { CosmosClient } from '@azure/cosmos'
import type { EntityMetadata } from './src/remult3/remult3.js'
import type {
  SqlCommand,
  SqlImplementation,
  SqlResult,
} from './src/sql-command.js'
import { SqlDatabase } from './src/data-providers/sql-database.js'
import type {
  CanBuildMigrations,
  MigrationBuilder,
} from './migrations/migration-types.js'

export interface CosmosConfig {
  endpoint: string
  key: string
  databaseId: string
  containerName?: string
}

export class CosmosDataProvider
  implements SqlImplementation, CanBuildMigrations
{
  private client: CosmosClient
  private container!: Container
  private initialized = false
  supportsJsonColumnType = true

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

  async end() {
    await this.client.dispose()
  }

  createCommand(): SqlCommand {
    return new CosmosCommand(this.container)
  }

  getLimitSqlSyntax(limit: number, offset: number) {
    return ` OFFSET ${offset} LIMIT ${limit}`
  }

  async transaction(run: (dp: SqlImplementation) => Promise<void>) {
    await run(this) // Cosmos doesn't support multi-statement SQL transactions, so just run.
  }

  async entityIsUsedForTheFirstTime(_entity: EntityMetadata) {
    if (!this.initialized) await this.init()
  }

  provideMigrationBuilder(): MigrationBuilder {
    return {
      addColumn: async () => {},
      createTable: async () => {},
    }
  }

  async ensureSchema() {
    if (!this.initialized) await this.init()
  }
}

class CosmosCommand implements SqlCommand {
  private params: any[] = []
  constructor(private container: Container) {}

  addParameterAndReturnSqlToken(val: any) {
    this.params.push(val)
    return `@p${this.params.length}`
  }
  param(val: any) {
    return this.addParameterAndReturnSqlToken(val)
  }

  async execute(sql: string): Promise<SqlResult> {
    const lower = sql.toLowerCase()
    if (lower.startsWith('insert into')) return this.insert(sql)
    if (lower.startsWith('update')) return this.update(sql)
    if (lower.startsWith('delete from')) return this.delete(sql)
    return this.select(sql)
  }

  private async insert(sql: string): Promise<SqlResult> {
    const match = sql.match(/\(([^)]*)\)/)
    if (!match) throw new Error('Invalid INSERT statement')

    const cols = match[1].split(',').map((c) => c.trim())
    const doc = cols.reduce(
      (a, c, i) => ((a[c] = this.params[i]), a),
      {} as any,
    )
    const { resource } = await this.container.items.create(doc)
    return new CosmosQueryResult([resource])
  }

  private async update(sql: string): Promise<SqlResult> {
    const set = sql.match(/set\s+([^]*?)\s+where/i)
    const wh = sql.match(/where\s+([^]*?)(?:\s+returning|$)/i)
    if (!set || !wh) throw new Error('Invalid UPDATE statement')

    const updates = this.parseSetClause(set[1])
    const id = this.parseWhereClause(wh[1])
    const doc = await this.findDocumentById(id)
    if (!doc) throw new Error('Document not found')

    updates.forEach(({ field, value }) => (doc[field] = value))
    const { resource } = await this.container.items.upsert(doc)
    return new CosmosQueryResult([resource])
  }

  private async delete(sql: string): Promise<SqlResult> {
    const wh = sql.match(/where\s+([^]*?)(?:\s+returning|$)/i)
    if (!wh) throw new Error('Invalid DELETE statement')

    const id = this.parseWhereClause(wh[1])
    const { resources } = await this.container.items
      .query({
        query: 'SELECT * FROM c WHERE c.id = @id',
        parameters: [{ name: '@id', value: id }],
      })
      .fetchAll()

    if (!resources.length) return new CosmosQueryResult([])
    await this.container.item(resources[0].id).delete()
    return new CosmosQueryResult(resources)
  }

  private async select(sql: string): Promise<SqlResult> {
    const querySpec = this.buildSelectQuery(sql)
    const { resources } = await this.container.items.query(querySpec).fetchAll()
    return new CosmosQueryResult(resources)
  }

  private parseSetClause(setClause: string) {
    return setClause.split(',').map((pair) => {
      const [field, param] = pair.split('=').map((s) => s.trim())
      const idx = parseInt(param.replace('@p', ''), 10) - 1
      return { field, value: this.params[idx] }
    })
  }

  private parseWhereClause(whereClause: string) {
    // e.g., "id = @p1" => parse the integer from '@p1'
    const idx = parseInt(whereClause.split('@p')[1], 10) - 1
    return this.params[idx]
  }

  private async findDocumentById(id: string) {
    const { resources } = await this.container.items
      .query({
        query: 'SELECT * FROM c WHERE c.id = @id',
        parameters: [{ name: '@id', value: id }],
      })
      .fetchAll()
    return resources[0]
  }

  private buildSelectQuery(sql: string) {
    let query = 'SELECT * FROM c'
    const parameters: { name: string; value: any }[] = []

    if (sql.toLowerCase().includes('where')) {
      // everything after "WHERE", up to "ORDER BY", "LIMIT", or "OFFSET"
      const [_, afterWhere] = sql.split(/where/i)
      const seg = afterWhere.split(/order by|limit|offset/i)[0].trim()

      const replaced = seg.replace(
        /(\w+)\s*=\s*@p(\d+)/gi,
        (_, field, pIndex) => {
          const idx = parseInt(pIndex, 10) - 1
          const paramName = `@p${idx}`
          parameters.push({ name: paramName, value: this.params[idx] })
          return `c.${field} = ${paramName}`
        },
      )

      query += ` WHERE ${replaced}`
    }

    query = this.appendLimitOffset(sql, query)
    return { query, parameters }
  }

  private appendLimitOffset(sql: string, query: string) {
    const limit = sql.match(/limit\s+(\d+)/i)
    const offset = sql.match(/offset\s+(\d+)/i)
    const offVal = offset ? parseInt(offset[1], 10) : 0
    if (limit) query += ` OFFSET ${offVal} LIMIT ${parseInt(limit[1], 10)}`
    return query
  }
}

class CosmosQueryResult implements SqlResult {
  constructor(public rows: any[]) {
    this.rows = rows.map(
      ({ _rid, _self, _etag, _attachments, _ts, ...rest }) => rest,
    )
  }
  getColumnKeyInResultForIndexInSelect(i: number) {
    return this.rows.length ? Object.keys(this.rows[0])[i] || '' : ''
  }
}

export async function createCosmosConnection(config: CosmosConfig) {
  const provider = new CosmosDataProvider(config)
  await provider.init()
  return new SqlDatabase(provider)
}
