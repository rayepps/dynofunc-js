import dynameh from 'dynameh'

import _ from '@x-recon/fn'


export const getItem = async ({ dynamo, table, hashKey, sortKey = null }) => {
  const request = dynameh.requestBuilder.buildGetInput(table, hashKey, sortKey)
  const response = await dynamo.getItem(request).promise()
  return dynameh.responseUnwrapper.unwrapGetOutput(response)
}

export const batchGetItems = async ({ dynamo, table, keyList }) => {
  const request = dynameh.requestBuilder.buildBatchGetInput(table, keyList)
  const response = await dynamo.batchGetItem(request).promise()
  return dynameh.responseUnwrapper.unwrapBatchGetOutput(response)
}

export const putItem = async ({ dynamo, table, item }) => {
  const request = dynameh.requestBuilder.buildPutInput(table, item)
  await dynamo.putItem(request).promise()
}

export const updateItem = async ({ dynamo, table, hashKey, sortKey = null, update }) => {
    
  const item = await getItem({
    dynamo,
    table,
    hashKey,
    sortKey
  })
  
  const newItem = _.isFunction(update)
    ? update(item)
    : { ...item, ...update }
  
  await putItem({
    dynamo,
    table,
    item: newItem
  })

  return newItem
}

export const query = async ({ dynamo, table, hashKey, sortKey, operation = '=' }) => {
  const request = dynameh.requestBuilder.buildQueryInput(table, hashKey, operation, sortKey)
  const response = await dynamo.query(request).promise()
  return dynameh.responseUnwrapper.unwrapQueryOutput(response)
}

export const scanByCallback = async ({ dynamo, table, onItems }) => {
  const scanInput = dynameh.requestBuilder.buildScanInput(table)
  await dynameh.scanHelper.scanByCallback(dynamo, scanInput, onItems)
}

export const deleteItem = async ({ dynamo, table, item }) => {
  const deleteInput = dynameh.requestBuilder.buildDeleteInput(table, item)
  await dynamo.deleteItem(deleteInput).promise()
}

export const deleteItemByKey = async ({ dynamo, table, hashKey, sortKey = null }) => {
  const { partitionKeyField: hk, sortKeyField: sk } = table
  const item = sortKey
    ? { [hk]: hashKey, [sk]: sortKey }
    : { [hk]: hashKey }
  await deleteItem({ dynamo, table, item})
}

export const deleteItemsByQuery = async ({ dynamo, table, hashKey, sortKey, operation = '=' }) => {
  
  const { partitionKeyField, sortKeyField } = table

  const makeKey = (item) => {
    return sortKeyField
      ? [item[partitionKeyField], item[sortKeyField]]
      : [item[partitionKeyField]]
  }

  const deleteBatch = async (items) => {   
    if (items?.length < 1) return
    const batchDeleteInput = dynameh.requestBuilder.buildBatchDeleteInput(table, items.map(makeKey))
    await dynameh.batchHelper.batchWriteAll(dynamo, batchDeleteInput)
  }

  const queryInput = dynameh.requestBuilder.buildQueryInput(table, hashKey, operation, sortKey)
  await dynameh.queryHelper.queryByCallback(dynamo, queryInput, deleteBatch)

}

const client = (dynamo, table = null) => ({
  getItem:            _.partob(getItem,             { dynamo, table }),
  putItem:            _.partob(putItem,             { dynamo, table }),
  query:              _.partob(query,               { dynamo, table }),
  scanByCallback:     _.partob(scanByCallback,      { dynamo, table }),
  deleteItemByKey:    _.partob(deleteItemByKey,     { dynamo, table }),
  deleteItem:         _.partob(deleteItem,          { dynamo, table }),
  deleteItemsByQuery: _.partob(deleteItemsByQuery,  { dynamo, table }),
  updateItem:         _.partob(updateItem,          { dynamo, table }),
  batchGetItems:      _.partob(batchGetItems,       { dynamo, table })
})

export default client