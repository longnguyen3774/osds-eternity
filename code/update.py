from update_data import transactions_collection

max_block = transactions_collection.aggregate([
    {'$group': {'_id': None, 'maxBlock': {'$max': '$block'}}}
])
for block in max_block:
    print(block['maxBlock'])