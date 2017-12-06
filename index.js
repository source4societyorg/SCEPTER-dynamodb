
const DynamoDB = function () {
  this.client = null
  this.AWS = require('aws-sdk')
}

DynamoDB.prototype.setConfiguration = function (configFile, env) {
  if (configFile[env].provider !== 'aws') {
    throw new Error('The configuration provider must be of type aws')
  }
  this.configuration = configFile[env].configuration
  this.AWS.config.update(this.configuration)
  this.instantiateClient()
}

DynamoDB.prototype.instantiateClient = function () {
  this.client = new this.AWS.DynamoDB.DocumentClient({apiVersion: '2012-10-08'})
}

DynamoDB.prototype.query = function (tableName = null, expressionAttributes, keyConditions, filterExpressions, callback) {
  const params = {
    ExpressionAttributeValues: expressionAttributes,
    KeyConditionExpression: keyConditions,
    FilterExpression: filterExpressions,
    TableName: tableName
  }

  this.client.query(params, callback)
}

DynamoDB.prototype.getItem = function (tableName, key, projectionExpression, callback) {
  const params = {
    TableName: tableName,
    Key: key,
    ProjectionExpression: projectionExpression
  }

  this.client.getItem(params, callback)
}

DynamoDB.prototype.putItem = function (tableName, item, callback) {
  const params = {
    TableName: tableName,
    Item: item
  }

  this.client.putItem(params, callback)
}

DynamoDB.prototype.deleteItem = function (tableName, key, callback) {
  const params = {
    TableName: tableName,
    Key: key
  }

  this.client.deleteItem(params, callback)
}

module.exports = DynamoDB
