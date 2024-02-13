import os
import json
from operator import itemgetter

import utils

visualization_url = os.getenv('visualization_url')

default_strategy = 'cost'
default_balanced_weight = 0.5
optimization_strategies = {
    'cost': lambda: find_cheapest,
    'speed': lambda: find_fastest,
    'balanced': lambda: find_balanced,
}

def handler(event, context):
    if not isinstance(event.get('stats'), list) or not event.get('stats'):
        raise ValueError('Wrong input ' + json.dumps(event))

    if event.get('dryRun'):
        print('[Dry-run] Skipping analysis')
        return

    return find_optimal_configuration(event)

def get_strategy(event):
    return event.get('strategy') or default_strategy

def get_balanced_weight(event):
    weight = event.get('balancedWeight')
    if weight is None:
        weight = default_balanced_weight
    return min(max(weight, 0.0), 1.0)

def find_optimal_configuration(event):
    stats = extract_statistics(event)
    strategy = get_strategy(event)
    balanced_weight = get_balanced_weight(event)
    optimization_function = optimization_strategies[strategy]()
    optimal = optimization_function(stats, balanced_weight)

    optimal['stateMachine'] = {}
    optimal['stateMachine']['executionCost'] = utils.step_functions_cost(len(event['stats']))
    optimal['stateMachine']['lambdaCost'] = sum(p['totalCost'] for p in stats)
    optimal['stateMachine']['visualization'] = utils.build_visualization_url(stats, visualization_url)

    del optimal['totalCost']

    return optimal

def extract_statistics(event):
    return [
        {
            'power': stat['value'],
            'cost': stat['averagePrice'],
            'duration': stat['averageDuration'],
            'totalCost': stat['totalCost'],
        }
        for stat in event['stats'] if stat and stat.get('averageDuration')
    ]

def find_cheapest(stats):
    print('Finding cheapest')
    stats.sort(key=itemgetter('cost', 'duration'))
    print('Stats: ', stats)
    return stats[0]

def find_fastest(stats):
    print('Finding fastest')
    stats.sort(key=itemgetter('duration', 'cost'))
    print('Stats: ', stats)
    return stats[0]

def find_balanced(stats, weight):
    print('Finding balanced configuration with balancedWeight = ', weight)
    max_cost = max(x['cost'] for x in stats)
    max_duration = max(x['duration'] for x in stats)
    get_value = lambda x: weight * x['cost'] / max_cost + (1 - weight) * x['duration'] / max_duration
    stats.sort(key=get_value)
    print('Stats: ', stats)
    return stats[0]