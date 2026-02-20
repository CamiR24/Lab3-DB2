//ejercicio 2.6


[
    {
        '$unwind': '$transactions'
    }, {
        '$addFields': {
            'month': {
                '$dateToString': {
                    'format': '%Y-%m', 
                    'date': '$transactions.date'
                }
            }, 
            'transaction_code': '$transactions.transaction_code', 
            'amount': '$transactions.amount'
        }
    }, {
        '$group': {
            '_id': {
                'month': '$month', 
                'transaction_code': '$transaction_code'
            }, 
            'total_amount': {
                '$sum': '$amount'
            }, 
            'avg_amount': {
                '$avg': '$amount'
            }
        }
    }
]