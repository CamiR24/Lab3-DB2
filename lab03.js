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