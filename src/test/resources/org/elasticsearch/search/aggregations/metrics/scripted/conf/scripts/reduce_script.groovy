newaggregation = []; sum = 0;for (aggregation in _aggs) { for (a in aggregation) { sum += a} }; newaggregation.add(sum); return newaggregation