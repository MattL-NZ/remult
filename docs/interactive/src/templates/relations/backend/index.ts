import express from 'express'
import { remultExpress } from 'remult/remult-express'
import { Customer } from './shared/Customer'
import { Order } from './shared/Order'
import { seedData } from './shared/SeedData'
import { createSqlite3DataProvider } from 'remult/remult-sqlite3'

export const app = express()
export const api = remultExpress({
  entities: [Order, Customer],
  admin: true,
  dataProvider: createSqlite3DataProvider(),
  initApi: async () => {
    await seedData()
  },
})

app.use(api)
