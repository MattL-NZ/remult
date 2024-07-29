import {
  type EntityDataProvider,
  type EntityMetadata,
  InMemoryDataProvider,
  type DataProvider,
} from '../../core'

export type TestDataProvider = InMemoryDataProvider & {
  finds: { entity: any; where: any }[]
}
export function TestDataProvider<T extends DataProvider = InMemoryDataProvider>(
  dataProvider?: T,
) {
  let finds: { entity: any; where: any }[] = []
  let updates: { entity: string; id: any; data: any }[] = []
  if (!dataProvider) dataProvider = new InMemoryDataProvider() as any as T
  const result = new Proxy(dataProvider, {
    //@ts-ignore
    get(target: T, p: keyof T) {
      if (p === 'getEntityDataProvider') {
        return (e: EntityMetadata) =>
          new Proxy(dataProvider.getEntityDataProvider(e), {
            get(target, p: keyof EntityDataProvider) {
              if (p === 'find')
                return (x: any) => {
                  finds.push({
                    entity: e.key,
                    where: x?.where?.toJson(),
                  })
                  return target[p](x)
                }
              else if (p === 'update') {
                return (id: any, data: any) => {
                  updates.push({ entity: e.key, id, data })
                  return target[p](id, data)
                }
              }
              return target[p]
            },
          })
      }
      return target[p]
    },
  })
  return Object.assign(result, { finds, updates })
}
