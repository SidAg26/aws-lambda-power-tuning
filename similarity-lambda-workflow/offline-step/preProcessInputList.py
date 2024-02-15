def lambda_handler(event, context):
    input_dict = event['payload']
    if isinstance(input_dict, dict):
        i_min = input_dict['min'] if 'min' in input_dict else 0
        i_max = input_dict['max'] if 'max' in input_dict else 100
        i_step = input_dict['stepSize'] if 'stepSize' in input_dict else None
    else:
        i_min = 1
        i_max = 100
        i_step = None
    if i_step:
        input_list = [i for i in range(i_min, i_max, i_step)]
    else:
        input_list = [i_min, i_max]

    return input_list