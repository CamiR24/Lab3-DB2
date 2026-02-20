//ejercicio 2.1
[
  {
    '$lookup': {
      'from': 'transactions', 
      'localField': 'accounts', 
      'foreignField': 'account_id', 
      'as': 'transactions_info'
    }
  }, {
    '$unwind': {
      'path': '$transactions_info'
    }
  }, {
    '$unwind': {
      'path': '$transactions_info.transactions'
    }
  }, {
    '$group': {
      '_id': {
        'customer_id': '$_id', 
        'name': '$name', 
        'address': '$address'
      }, 
      'total_transactions': {
        '$sum': 1
      }, 
      'average_amount': {
        '$avg': '$transactions_info.transactions.amount'
      }
    }
  }, {
    '$project': {
      '_id': 0, 
      'name': '$_id.name', 
      'city': {
        '$trim': {
          'input': {
            '$arrayElemAt': [
              {
                '$split': [
                  '$_id.address', ','
                ]
              }, 1
            ]
          }
        }
      }, 
      'total_transactions': 1, 
      'average_amount': {
        '$round': [
          '$average_amount', 2
        ]
      }
    }
  }, {
    '$sort': {
      'total_transactions': -1
    }
  }
]

//ejercicio 2.2
[
  {
    '$lookup': {
      'from': 'transactions', 
      'localField': 'accounts', 
      'foreignField': 'account_id', 
      'as': 'transactions_info'
    }
  }, {
    '$unwind': {
      'path': '$transactions_info'
    }
  }, {
    '$unwind': {
      'path': '$transactions_info.transactions'
    }
  }, {
    '$group': {
      '_id': {
        'customer_id': '$_id', 
        'name': '$name'
      }, 
      'total_buys': {
        '$sum': {
          '$cond': [
            {
              '$eq': [
                '$transactions_info.transactions.transaction_code', 'buy'
              ]
            }, {
              '$toDouble': '$transactions_info.transactions.total'
            }, 0
          ]
        }
      }, 
      'total_sells': {
        '$sum': {
          '$cond': [
            {
              '$eq': [
                '$transactions_info.transactions.transaction_code', 'sell'
              ]
            }, {
              '$toDouble': '$transactions_info.transactions.total'
            }, 0
          ]
        }
      }
    }
  }, {
    '$project': {
      '_id': 0, 
      'name': '$_id.name', 
      'balance': {
        '$subtract': [
          '$total_sells', '$total_buys'
        ]
      }
    }
  }, {
    '$project': {
      'name': 1, 
      'balance': 1, 
      'category': {
        '$switch': {
          'branches': [
            {
              'case': {
                '$lt': [
                  '$balance', 5000
                ]
              }, 
              'then': 'Bajo'
            }, {
              'case': {
                '$and': [
                  {
                    '$gte': [
                      '$balance', 5000
                    ]
                  }, {
                    '$lte': [
                      '$balance', 20000
                    ]
                  }
                ]
              }, 
              'then': 'Medio'
            }, {
              'case': {
                '$gt': [
                  '$balance', 20000
                ]
              }, 
              'then': 'Alto'
            }
          ], 
          'default': 'Sin categoría'
        }
      }
    }
  }
]

//ejercicio 2.3
[
  {
    '$lookup': {
      'from': 'transactions', 
      'localField': 'accounts', 
      'foreignField': 'account_id', 
      'as': 'transactions_info'
    }
  }, {
    '$unwind': {
      'path': '$transactions_info'
    }
  }, {
    '$unwind': {
      'path': '$transactions_info.transactions'
    }
  }, {
    '$group': {
      '_id': {
        'customer_id': '$_id', 
        'name': '$name', 
        'address': '$address'
      }, 
      'total_buys': {
        '$sum': {
          '$cond': [
            {
              '$eq': [
                '$transactions_info.transactions.transaction_code', 'buy'
              ]
            }, {
              '$toDouble': '$transactions_info.transactions.total'
            }, 0
          ]
        }
      }, 
      'total_sells': {
        '$sum': {
          '$cond': [
            {
              '$eq': [
                '$transactions_info.transactions.transaction_code', 'sell'
              ]
            }, {
              '$toDouble': '$transactions_info.transactions.total'
            }, 0
          ]
        }
      }
    }
  }, {
    '$project': {
      '_id': 0, 
      'name': '$_id.name', 
      'city': {
        '$trim': {
          'input': {
            '$arrayElemAt': [
              {
                '$split': [
                  '$_id.address', ','
                ]
              }, 1
            ]
          }
        }
      }, 
      'total_balance': {
        '$subtract': [
          '$total_sells', '$total_buys'
        ]
      }
    }
  }, {
    '$sort': {
      'total_balance': -1
    }
  }, {
    '$group': {
      '_id': '$city', 
      'name': {
        '$first': '$name'
      }, 
      'total_balance': {
        '$first': '$total_balance'
      }
    }
  }, {
    '$project': {
      '_id': 0, 
      'city': '$_id', 
      'name': 1, 
      'total_balance': 1
    }
  }
]

//ejercicio 2.8
db.accounts.aggregate([
  { $unwind: "$products" },
  {
    $group: {
      _id: "$products",
      total_accounts: { $sum: 1 },
      average_limit: { $avg: "$limit" }
    }
  },
  {
    $project: {
      _id: 0,
      account_type: "$_id",
      total_accounts: 1,
      average_limit: { $round: ["$average_limit", 2] }
    }
  },
  { $out: "account_summaries" }
])


//ejercicio 2.9
db.customers.aggregate([

  { $unwind: "$accounts" },

  
  {
    $lookup: {
      from: "transactions",
      localField: "accounts",
      foreignField: "account_id",
      as: "tx"
    }
  },
  { $unwind: "$tx" },
  { $unwind: "$tx.transactions" },


  {
    $addFields: {
      signed_amount: {
        $cond: [
          { $eq: ["$tx.transactions.transaction_code", "sell"] },
          { $toDouble: "$tx.transactions.total" },
          { $multiply: [ { $toDouble: "$tx.transactions.total" }, -1 ] }
        ]
      }
    }
  },


  {
    $group: {
      _id: {
        name: "$name",
        email: "$email"
      },
      total_balance: { $sum: "$signed_amount" },
      transaction_count: { $sum: 1 }
    }
  },


  {
    $match: {
      total_balance: { $gt: 30000 },
      transaction_count: { $gt: 5 }
    }
  },


  {
    $project: {
      _id: 0,
      name: "$_id.name",
      email: "$_id.email",
      total_balance: { $round: ["$total_balance", 2] },
      transaction_count: 1
    }
  },

  { $out: "high_value_customers" }

])

//ejercicio 2.10

var maxDate = db.transactions.aggregate([
  { $unwind: "$transactions" },
  { $group: { _id: null, maxDate: { $max: "$transactions.date" } } }
]).toArray()[0].maxDate


db.customers.aggregate([

  { $unwind: "$accounts" },

  {
    $lookup: {
      from: "transactions",
      localField: "accounts",
      foreignField: "account_id",
      as: "tx"
    }
  },

  { $unwind: "$tx" },
  { $unwind: "$tx.transactions" },

  {
    $match: {
      "tx.transactions.date": {
        $gte: new Date(maxDate.getFullYear()-1, maxDate.getMonth(), maxDate.getDate())
      }
    }
  },

  {
    $group: {
      _id: "$name",
      total_transactions: { $sum: 1 }
    }
  },

  {
    $addFields: {
      monthly_avg: { $divide: ["$total_transactions", 12] }
    }
  },

  {
    $addFields: {
      category: {
        $switch: {
          branches: [
            { case: { $lt: ["$monthly_avg", 2] }, then: "infrequent" },
            {
              case: {
                $and: [
                  { $gte: ["$monthly_avg", 2] },
                  { $lte: ["$monthly_avg", 5] }
                ]
              },
              then: "regular"
            },
            { case: { $gt: ["$monthly_avg", 5] }, then: "frequent" }
          ],
          default: "infrequent"
        }
      }
    }
  },

  {
    $project: {
      _id: 0,
      name: "$_id",
      monthly_avg: { $round: ["$monthly_avg", 1] },
      category: 1
    }
  }

])