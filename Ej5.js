//ejercicio 2.5


[
    {
        '$project': {
            'account_id': 1, 
            'transactions': 1
        }
    }, {
        '$project': {
            'account_id': 1, 
            'transactions': {
                '$filter': {
                    'input': '$transactions', 
                    'as': 't', 
                    'cond': {
                        '$ne': [
                            '$$t.amount', None
                        ]
                    }
                }
            }
        }
    }, {
        '$match': {
            'transactions.1': {
                '$exists': True
            }
        }
    }, {
        '$project': {
            'account_id': 1, 
            'sortedTransactions': {
                '$sortArray': {
                    'input': '$transactions', 
                    'sortBy': {
                        'date': 1
                    }
                }
            }
        }
    }, {
        '$project': {
            'account_id': 1, 
            'oldestAmount': {
                '$arrayElemAt': [
                    '$sortedTransactions.amount', 0
                ]
            }, 
            'newestAmount': {
                '$arrayElemAt': [
                    '$sortedTransactions.amount', {
                        '$subtract': [
                            {
                                '$size': '$sortedTransactions'
                            }, 1
                        ]
                    }
                ]
            }
        }
    }, {
        '$project': {
            'account_id': 1, 
            'percent_variation': {
                '$cond': [
                    {
                        '$ne': [
                            '$oldestAmount', 0
                        ]
                    }, {
                        '$multiply': [
                            {
                                '$divide': [
                                    {
                                        '$subtract': [
                                            '$newestAmount', '$oldestAmount'
                                        ]
                                    }, '$oldestAmount'
                                ]
                            }, 100
                        ]
                    }, None
                ]
            }
        }
    }
]