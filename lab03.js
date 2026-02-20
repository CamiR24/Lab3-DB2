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