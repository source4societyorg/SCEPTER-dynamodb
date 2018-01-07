
const DynamoDB = function () {
  this.client = null
  this.AWS = require('aws-sdk')
}

DynamoDB.prototype.setConfiguration = function (configFile, env) {
  if (configFile.environments[env].provider !== 'aws') {
    throw new Error('The configuration provider must be of type aws')
  }
  this.configuration = configFile.environments[env].configuration
  this.AWS.config.update(this.configuration)
  this.instantiateClient()
}

DynamoDB.prototype.instantiateClient = function () {
  this.client = new this.AWS.DynamoDB.DocumentClient({apiVersion: '2012-10-08'})
}

DynamoDB.prototype.query = function (tableName = null, keyConditions, callback, options = {}, preserveLimit = true) {
  const basic = {
    KeyConditionExpression: keyConditions,
    TableName: tableName
  }
  const params = Object.assign(basic, options)

  let limit = options.Limit || undefined
  let completeData = { Items: [], Count: 0, ScannedCount: 0 }
  preserveLimit = preserveLimit && typeof limit !== 'undefined'

  const queryCallback = (err, data) => {
    if (typeof err === 'undefined' || err === null) {
      completeData.ScannedCount += data.ScannedCount
      completeData.Count += data.Items.length
      completeData.Items = completeData.Items.concat(data.Items)
      completeData.LastEvaluatedKey = data.LastEvaluatedKey
      if (preserveLimit && completeData.Count < limit && typeof data.LastEvaluatedKey !== 'undefined') {
        params.ExclusiveStartKey = data.LastEvaluatedKey
        this.client.query(params, queryCallback)
      } else {
        callback(null, completeData)
      }
    } else {
      callback(err)
    }
  }

  this.client.query(params, queryCallback)
}

DynamoDB.prototype.batchGet = function (tableName, requestItems, projectionExpression, callback, options = {}) {
  const basic = {
    TableName: tableName,
    RequestItems: requestItems,
    ProjectionExpression: projectionExpression
  }
  const params = Object.assign(basic, options)

  this.client.batchGet(params, callback)
}

DynamoDB.prototype.getItem = function (tableName, key, projectionExpression, callback, options = {}) {
  const basic = {
    TableName: tableName,
    Key: key,
    ProjectionExpression: projectionExpression
  }
  const params = Object.assign(basic, options)

  this.client.get(params, callback)
}

DynamoDB.prototype.putItem = function (tableName, item, callback, options = {}) {
  const basic = {
    TableName: tableName,
    Item: item
  }
  const params = Object.assign(basic, options)
  this.client.put(params, callback)
}

DynamoDB.prototype.deleteItem = function (tableName, key, callback) {
  const params = {
    TableName: tableName,
    Key: key
  }

  this.client.delete(params, callback)
}

module.exports = DynamoDB
