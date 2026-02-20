//ejercicio 2.7
[
    {
        '$lookup': {
            'from': 'transactions', 
            'localField': 'accounts', 
            'foreignField': 'account_id', 
            'as': 'matched_transactions'
        }
    }, {
        '$match': {
            'matched_transactions': {
                '$eq': []
            }
        }
    }, {
        '$project': {
            'matched_transactions': 0
        }
    }, {
        '$merge': {
            'into': 'inactive_customers'
        }
    }
]